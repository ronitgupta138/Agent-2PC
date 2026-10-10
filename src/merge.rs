//! Three-Way Semantic Merge Engine for Concurrent Agent Workspaces.

use std::fmt;

/// Result of a 3-way merge between base, ours (committed), and theirs (staged).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeResult {
    /// Disjoint non-overlapping edits merged cleanly with zero conflicts
    Clean(String),
    /// Overlapping edits detected; contains formatted conflict markers
    Conflict {
        merged_with_markers: String,
        conflict_count: usize,
    },
}

impl fmt::Display for MergeResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MergeResult::Clean(s) => write!(f, "{}", s),
            MergeResult::Conflict {
                merged_with_markers,
                conflict_count,
            } => write!(
                f,
                "CONFLICT({} conflicts)\n{}",
                conflict_count, merged_with_markers
            ),
        }
    }
}

pub struct ThreeWayMerger;

impl ThreeWayMerger {
    /// Perform a 3-way line-based merge between base, ours (committed), and theirs (staged).
    pub fn merge(base: &str, ours: &str, theirs: &str) -> MergeResult {
        // Fast paths
        if ours == theirs {
            return MergeResult::Clean(ours.to_string());
        }
        if base == ours {
            return MergeResult::Clean(theirs.to_string());
        }
        if base == theirs {
            return MergeResult::Clean(ours.to_string());
        }

        let base_lines: Vec<&str> = base.lines().collect();
        let ours_lines: Vec<&str> = ours.lines().collect();
        let theirs_lines: Vec<&str> = theirs.lines().collect();

        // Calculate simple line-level diff hunks against base
        let ours_diff = compute_diff(&base_lines, &ours_lines);
        let theirs_diff = compute_diff(&base_lines, &theirs_lines);

        let mut result = Vec::new();
        let mut base_idx = 0;
        let mut conflict_count = 0;

        let mut o_hunk_idx = 0;
        let mut t_hunk_idx = 0;

        while base_idx < base_lines.len()
            || o_hunk_idx < ours_diff.len()
            || t_hunk_idx < theirs_diff.len()
        {
            let next_o = ours_diff.get(o_hunk_idx);
            let next_t = theirs_diff.get(t_hunk_idx);

            match (next_o, next_t) {
                (Some(o), Some(t)) => {
                    // Check if both touch the same base index
                    if o.base_start == t.base_start {
                        if o.new_lines == t.new_lines {
                            // Identical edits: clean merge
                            result.extend(o.new_lines.iter().cloned());
                            base_idx = o.base_start + o.base_len;
                        } else {
                            // Overlapping conflicting edits!
                            conflict_count += 1;
                            result.push("<<<<<<< OURS (Committed)".to_string());
                            result.extend(o.new_lines.iter().cloned());
                            result.push("=======".to_string());
                            result.extend(t.new_lines.iter().cloned());
                            result.push(">>>>>>> THEIRS (Staged)".to_string());
                            base_idx = o.base_start + o.base_len.max(t.base_len);
                        }
                        o_hunk_idx += 1;
                        t_hunk_idx += 1;
                    } else if o.base_start < t.base_start {
                        while base_idx < o.base_start && base_idx < base_lines.len() {
                            result.push(base_lines[base_idx].to_string());
                            base_idx += 1;
                        }
                        result.extend(o.new_lines.iter().cloned());
                        base_idx = o.base_start + o.base_len;
                        o_hunk_idx += 1;
                    } else {
                        while base_idx < t.base_start && base_idx < base_lines.len() {
                            result.push(base_lines[base_idx].to_string());
                            base_idx += 1;
                        }
                        result.extend(t.new_lines.iter().cloned());
                        base_idx = t.base_start + t.base_len;
                        t_hunk_idx += 1;
                    }
                }
                (Some(o), None) => {
                    while base_idx < o.base_start && base_idx < base_lines.len() {
                        result.push(base_lines[base_idx].to_string());
                        base_idx += 1;
                    }
                    result.extend(o.new_lines.iter().cloned());
                    base_idx = o.base_start + o.base_len;
                    o_hunk_idx += 1;
                }
                (None, Some(t)) => {
                    while base_idx < t.base_start && base_idx < base_lines.len() {
                        result.push(base_lines[base_idx].to_string());
                        base_idx += 1;
                    }
                    result.extend(t.new_lines.iter().cloned());
                    base_idx = t.base_start + t.base_len;
                    t_hunk_idx += 1;
                }
                (None, None) => {
                    while base_idx < base_lines.len() {
                        result.push(base_lines[base_idx].to_string());
                        base_idx += 1;
                    }
                }
            }
        }

        let merged_text = result.join("\n");

        if conflict_count == 0 {
            MergeResult::Clean(merged_text)
        } else {
            MergeResult::Conflict {
                merged_with_markers: merged_text,
                conflict_count,
            }
        }
    }
}

#[derive(Debug, Clone)]
struct DiffHunk {
    base_start: usize,
    base_len: usize,
    new_lines: Vec<String>,
}

fn compute_diff(base: &[&str], modified: &[&str]) -> Vec<DiffHunk> {
    let mut hunks = Vec::new();
    let mut b = 0;
    let mut m = 0;

    while b < base.len() && m < modified.len() {
        if base[b] == modified[m] {
            b += 1;
            m += 1;
        } else {
            let base_start = b;
            let mut sync_point = None;

            'search: for (lb, base_line) in base.iter().enumerate().skip(b) {
                for (lm, mod_line) in modified.iter().enumerate().skip(m) {
                    if base_line == mod_line {
                        sync_point = Some((lb, lm));
                        break 'search;
                    }
                }
            }

            match sync_point {
                Some((next_b, next_m)) => {
                    let new_lines = modified[m..next_m].iter().map(|s| s.to_string()).collect();
                    hunks.push(DiffHunk {
                        base_start,
                        base_len: next_b - base_start,
                        new_lines,
                    });
                    b = next_b;
                    m = next_m;
                }
                None => {
                    let new_lines = modified[m..].iter().map(|s| s.to_string()).collect();
                    hunks.push(DiffHunk {
                        base_start,
                        base_len: base.len() - base_start,
                        new_lines,
                    });
                    b = base.len();
                    m = modified.len();
                }
            }
        }
    }

    if b < base.len() {
        hunks.push(DiffHunk {
            base_start: b,
            base_len: base.len() - b,
            new_lines: Vec::new(),
        });
    } else if m < modified.len() {
        let remaining_new: Vec<String> = modified[m..].iter().map(|s| s.to_string()).collect();
        hunks.push(DiffHunk {
            base_start: b,
            base_len: 0,
            new_lines: remaining_new,
        });
    }

    hunks
}
