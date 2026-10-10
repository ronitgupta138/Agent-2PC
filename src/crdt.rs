//! Character-Level Sequence CRDT Engine for Multi-Agent Concurrent Workspaces.

use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::HashMap;

/// Unique Lamport causal timestamp for an atomic character mutation.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LamportId {
    pub client: String,
    pub seq: u64,
}

impl PartialOrd for LamportId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for LamportId {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.seq.cmp(&other.seq) {
            Ordering::Equal => self.client.cmp(&other.client),
            ord => ord,
        }
    }
}

/// Character node in the CRDT document sequence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrdtNode {
    pub id: LamportId,
    pub parent: Option<LamportId>,
    pub ch: char,
    pub deleted: bool,
}

/// In-memory character sequence CRDT document supporting mathematical convergence.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CrdtDocument {
    nodes: Vec<CrdtNode>,
    clock: u64,
}

impl CrdtDocument {
    /// Create a new empty CRDT document.
    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
            clock: 0,
        }
    }

    /// Initialize CRDT document from existing text with initial author client.
    pub fn from_text(text: &str, client: &str) -> Self {
        let mut doc = Self::new();
        let mut prev_id = None;

        for ch in text.chars() {
            doc.clock += 1;
            let id = LamportId {
                client: client.to_string(),
                seq: doc.clock,
            };
            doc.nodes.push(CrdtNode {
                id: id.clone(),
                parent: prev_id,
                ch,
                deleted: false,
            });
            prev_id = Some(id);
        }

        doc
    }

    /// Insert a character at visible text offset.
    pub fn insert(&mut self, visible_offset: usize, ch: char, client: &str) -> LamportId {
        self.clock += 1;
        let id = LamportId {
            client: client.to_string(),
            seq: self.clock,
        };

        let parent = self.get_parent_at_offset(visible_offset);

        self.nodes.push(CrdtNode {
            id: id.clone(),
            parent,
            ch,
            deleted: false,
        });

        id
    }

    /// Mark character at visible offset as deleted (tombstone).
    pub fn delete(&mut self, visible_offset: usize) -> bool {
        let target_id = self.get_node_id_at_offset(visible_offset);
        if let Some(id) = target_id
            && let Some(node) = self.nodes.iter_mut().find(|n| n.id == id) {
                node.deleted = true;
                return true;
        }
        false
    }

    /// Merge another agent's CRDT document mutations into this document.
    pub fn merge(&mut self, other: &CrdtDocument) {
        for other_node in &other.nodes {
            if let Some(existing) = self.nodes.iter_mut().find(|n| n.id == other_node.id) {
                if other_node.deleted {
                    existing.deleted = true;
                }
            } else {
                self.nodes.push(other_node.clone());
            }
        }
        self.clock = self.clock.max(other.clock);
    }

    /// Extract visible text representation using deterministic causal pre-order tree traversal.
    pub fn to_string_lossy(&self) -> String {
        let mut children: HashMap<Option<LamportId>, Vec<&CrdtNode>> = HashMap::new();
        for node in &self.nodes {
            children.entry(node.parent.clone()).or_default().push(node);
        }

        // Sort children by LamportId descending for deterministic tie breaking
        for list in children.values_mut() {
            list.sort_by(|a, b| b.id.cmp(&a.id));
        }

        let mut ordered_nodes = Vec::new();
        Self::dfs(None, &children, &mut ordered_nodes);

        ordered_nodes
            .into_iter()
            .filter(|n| !n.deleted)
            .map(|n| n.ch)
            .collect()
    }

    fn dfs<'a>(
        parent: Option<LamportId>,
        children: &HashMap<Option<LamportId>, Vec<&'a CrdtNode>>,
        out: &mut Vec<&'a CrdtNode>,
    ) {
        if let Some(nodes) = children.get(&parent) {
            for node in nodes {
                out.push(node);
                Self::dfs(Some(node.id.clone()), children, out);
            }
        }
    }

    fn get_parent_at_offset(&self, visible_offset: usize) -> Option<LamportId> {
        if visible_offset == 0 {
            return None;
        }

        let mut children: HashMap<Option<LamportId>, Vec<&CrdtNode>> = HashMap::new();
        for node in &self.nodes {
            children.entry(node.parent.clone()).or_default().push(node);
        }
        for list in children.values_mut() {
            list.sort_by(|a, b| b.id.cmp(&a.id));
        }

        let mut ordered_nodes = Vec::new();
        Self::dfs(None, &children, &mut ordered_nodes);

        let mut count = 0;
        for node in ordered_nodes {
            if !node.deleted {
                count += 1;
                if count == visible_offset {
                    return Some(node.id.clone());
                }
            }
        }

        None
    }

    fn get_node_id_at_offset(&self, visible_offset: usize) -> Option<LamportId> {
        let mut children: HashMap<Option<LamportId>, Vec<&CrdtNode>> = HashMap::new();
        for node in &self.nodes {
            children.entry(node.parent.clone()).or_default().push(node);
        }
        for list in children.values_mut() {
            list.sort_by(|a, b| b.id.cmp(&a.id));
        }

        let mut ordered_nodes = Vec::new();
        Self::dfs(None, &children, &mut ordered_nodes);

        let mut count = 0;
        for node in ordered_nodes {
            if !node.deleted {
                if count == visible_offset {
                    return Some(node.id.clone());
                }
                count += 1;
            }
        }

        None
    }
}
