// 多路归并迭代器：把 memtable 和多个 SSTable 的有序迭代器合并成一个全局有序流
//
// - 多个源包含同一个 key 时，优先级最高（最新）的版本胜出，其余版本被丢弃
// - 产出的值保留墓碑（None），由上层决定是跳过（读路径）还是保留（compaction）
// - 支持双向迭代（Engine trait 要求 DoubleEndedIterator，MVCC 靠 rev() 反向读版本链）

use crate::error::Result;

// 归并元素：key + 值，None 表示墓碑
pub type MergeItem = Result<(Vec<u8>, Option<Vec<u8>>)>;

// 一个归并源：底层迭代器 + 优先级（越大越新）
// front/back 是两端的 peek 缓冲，剩余元素的顺序始终为 [front, iter..., back]
pub struct Source<'a> {
    iter: Box<dyn DoubleEndedIterator<Item = MergeItem> + 'a>,
    priority: u64,
    front: Option<MergeItem>,
    back: Option<MergeItem>,
}

impl<'a> Source<'a> {
    pub fn new(iter: Box<dyn DoubleEndedIterator<Item = MergeItem> + 'a>, priority: u64) -> Self {
        Self {
            iter,
            priority,
            front: None,
            back: None,
        }
    }

    fn peek_front(&mut self) -> Option<&MergeItem> {
        if self.front.is_none() {
            // 底层迭代器耗尽后，最后一个元素可能停留在 back 缓冲中
            self.front = self.iter.next().or_else(|| self.back.take());
        }
        self.front.as_ref()
    }

    fn pop_front(&mut self) -> Option<MergeItem> {
        self.peek_front();
        self.front.take()
    }

    fn peek_back(&mut self) -> Option<&MergeItem> {
        if self.back.is_none() {
            self.back = self.iter.next_back().or_else(|| self.front.take());
        }
        self.back.as_ref()
    }

    fn pop_back(&mut self) -> Option<MergeItem> {
        self.peek_back();
        self.back.take()
    }
}

pub struct MergeIterator<'a> {
    sources: Vec<Source<'a>>,
    // 双向迭代时两端已产出的边界，防止同一个 key 被前后两端各产出一次
    front_yielded: Option<Vec<u8>>, // 前端已产出的最大 key
    back_yielded: Option<Vec<u8>>,  // 后端已产出的最小 key
}

impl<'a> MergeIterator<'a> {
    pub fn new(sources: Vec<Source<'a>>) -> Self {
        Self {
            sources,
            front_yielded: None,
            back_yielded: None,
        }
    }
}

impl<'a> Iterator for MergeIterator<'a> {
    type Item = MergeItem;

    fn next(&mut self) -> Option<Self::Item> {
        // 错误直接向上传播
        for s in self.sources.iter_mut() {
            if let Some(Err(_)) = s.peek_front() {
                return s.pop_front();
            }
        }
        // 找到所有源头部中最小的 key
        let mut min_key: Option<Vec<u8>> = None;
        for s in self.sources.iter_mut() {
            if let Some(Ok((k, _))) = s.peek_front() {
                if min_key.as_ref().map_or(true, |m| k < m) {
                    min_key = Some(k.clone());
                }
            }
        }
        let key = min_key?;
        // 后端已经产出过 >= key 的位置，说明两端相遇，迭代结束
        if let Some(bb) = &self.back_yielded {
            if key >= *bb {
                return None;
            }
        }
        // 从所有头部等于该 key 的源中弹出，优先级最高的版本胜出
        let mut best: Option<(u64, Option<Vec<u8>>)> = None;
        for s in self.sources.iter_mut() {
            let matched = matches!(s.peek_front(), Some(Ok((k, _))) if *k == key);
            if matched {
                if let Some(Ok((_, v))) = s.pop_front() {
                    if best.as_ref().map_or(true, |(p, _)| s.priority > *p) {
                        best = Some((s.priority, v));
                    }
                }
            }
        }
        self.front_yielded = Some(key.clone());
        best.map(|(_, v)| Ok((key, v)))
    }
}

impl<'a> DoubleEndedIterator for MergeIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        for s in self.sources.iter_mut() {
            if let Some(Err(_)) = s.peek_back() {
                return s.pop_back();
            }
        }
        // 找到所有源尾部中最大的 key
        let mut max_key: Option<Vec<u8>> = None;
        for s in self.sources.iter_mut() {
            if let Some(Ok((k, _))) = s.peek_back() {
                if max_key.as_ref().map_or(true, |m| k > m) {
                    max_key = Some(k.clone());
                }
            }
        }
        let key = max_key?;
        if let Some(fb) = &self.front_yielded {
            if key <= *fb {
                return None;
            }
        }
        let mut best: Option<(u64, Option<Vec<u8>>)> = None;
        for s in self.sources.iter_mut() {
            let matched = matches!(s.peek_back(), Some(Ok((k, _))) if *k == key);
            if matched {
                if let Some(Ok((_, v))) = s.pop_back() {
                    if best.as_ref().map_or(true, |(p, _)| s.priority > *p) {
                        best = Some((s.priority, v));
                    }
                }
            }
        }
        self.back_yielded = Some(key.clone());
        best.map(|(_, v)| Ok((key, v)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source(entries: Vec<(&[u8], Option<&[u8]>)>, priority: u64) -> Source<'static> {
        let items: Vec<MergeItem> = entries
            .into_iter()
            .map(|(k, v)| Ok((k.to_vec(), v.map(|v| v.to_vec()))))
            .collect();
        Source::new(Box::new(items.into_iter()), priority)
    }

    fn collect(iter: MergeIterator) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        iter.map(|r| r.unwrap()).collect()
    }

    #[test]
    fn test_merge_ordering() {
        let iter = MergeIterator::new(vec![
            source(vec![(b"a", Some(b"1")), (b"c", Some(b"3"))], 1),
            source(vec![(b"b", Some(b"2")), (b"d", Some(b"4"))], 2),
        ]);
        let items = collect(iter);
        let keys: Vec<&[u8]> = items.iter().map(|(k, _)| k.as_slice()).collect();
        assert_eq!(keys, vec![b"a", b"b", b"c", b"d"]);
    }

    #[test]
    fn test_merge_priority() {
        // 同一个 key，优先级高的（新的）胜出，包括墓碑
        let iter = MergeIterator::new(vec![
            source(vec![(b"a", Some(b"old")), (b"b", Some(b"old"))], 1),
            source(vec![(b"a", Some(b"new")), (b"b", None)], 2),
        ]);
        let items = collect(iter);
        assert_eq!(
            items,
            vec![
                (b"a".to_vec(), Some(b"new".to_vec())),
                (b"b".to_vec(), None),
            ]
        );
    }

    #[test]
    fn test_merge_reverse() {
        let iter = MergeIterator::new(vec![
            source(vec![(b"a", Some(b"1")), (b"c", Some(b"old"))], 1),
            source(vec![(b"b", Some(b"2")), (b"c", Some(b"new"))], 2),
        ]);
        let items: Vec<_> = iter.rev().map(|r| r.unwrap()).collect();
        assert_eq!(
            items,
            vec![
                (b"c".to_vec(), Some(b"new".to_vec())),
                (b"b".to_vec(), Some(b"2".to_vec())),
                (b"a".to_vec(), Some(b"1".to_vec())),
            ]
        );
    }

    #[test]
    fn test_merge_interleaved() {
        // 双向交替消费，验证两端不会重复或遗漏
        let mut iter = MergeIterator::new(vec![
            source(vec![(b"a", Some(b"1")), (b"c", Some(b"3"))], 1),
            source(vec![(b"b", Some(b"2")), (b"d", Some(b"4"))], 2),
        ]);
        assert_eq!(iter.next().unwrap().unwrap().0, b"a".to_vec());
        assert_eq!(iter.next_back().unwrap().unwrap().0, b"d".to_vec());
        assert_eq!(iter.next_back().unwrap().unwrap().0, b"c".to_vec());
        assert_eq!(iter.next().unwrap().unwrap().0, b"b".to_vec());
        assert!(iter.next().is_none());
        assert!(iter.next_back().is_none());
    }

    #[test]
    fn test_merge_interleaved_duplicate_key() {
        // 重复 key 出现在两端相遇处，不能被产出两次
        let mut iter = MergeIterator::new(vec![
            source(vec![(b"a", Some(b"1")), (b"b", Some(b"old"))], 1),
            source(vec![(b"b", Some(b"new")), (b"c", Some(b"3"))], 2),
        ]);
        assert_eq!(iter.next().unwrap().unwrap().0, b"a".to_vec());
        let (k, v) = iter.next_back().unwrap().unwrap();
        assert_eq!(k, b"c".to_vec());
        assert_eq!(v, Some(b"3".to_vec()));
        let (k, v) = iter.next_back().unwrap().unwrap();
        assert_eq!(k, b"b".to_vec());
        assert_eq!(v, Some(b"new".to_vec()));
        assert!(iter.next().is_none());
        assert!(iter.next_back().is_none());
    }
}
