use agent_2pc::crdt::CrdtDocument;

#[test]
fn test_crdt_concurrent_insertion_convergence() {
    let mut doc_a = CrdtDocument::from_text("fn test() {}", "init");
    let mut doc_b = doc_a.clone();

    // Agent A inserts inside brackets
    doc_a.insert(10, 'a', "agent_a");
    doc_a.insert(11, '1', "agent_a");

    // Agent B concurrently inserts inside brackets
    doc_b.insert(10, 'b', "agent_b");
    doc_b.insert(11, '2', "agent_b");

    // Replicate / merge mutually
    doc_a.merge(&doc_b);
    doc_b.merge(&doc_a);

    // CRDT Mathematical Invariant: Both must converge to identical string!
    let text_a = doc_a.to_string_lossy();
    let text_b = doc_b.to_string_lossy();

    assert_eq!(text_a, text_b);
    assert!(text_a.contains("a1"));
    assert!(text_a.contains("b2"));
}

#[test]
fn test_crdt_deletion_tombstone() {
    let mut doc = CrdtDocument::from_text("abcdef", "client1");
    assert_eq!(doc.to_string_lossy(), "abcdef");

    // Delete 'c' (offset 2)
    let deleted = doc.delete(2);
    assert!(deleted);
    assert_eq!(doc.to_string_lossy(), "abdef");

    // Delete 'a' (offset 0)
    let deleted_first = doc.delete(0);
    assert!(deleted_first);
    assert_eq!(doc.to_string_lossy(), "bdef");
}

#[test]
fn test_crdt_commutativity() {
    let base = CrdtDocument::from_text("abc", "init");

    let mut doc1 = base.clone();
    doc1.insert(1, 'X', "c1");

    let mut doc2 = base.clone();
    doc2.insert(2, 'Y', "c2");

    let mut merge1_into_2 = doc2.clone();
    merge1_into_2.merge(&doc1);

    let mut merge2_into_1 = doc1.clone();
    merge2_into_1.merge(&doc2);

    assert_eq!(merge1_into_2.to_string_lossy(), merge2_into_1.to_string_lossy());
}
