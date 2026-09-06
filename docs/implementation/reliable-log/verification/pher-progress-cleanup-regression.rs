    #[test]
    fn review_retired_listener_drops_applied_progress() {
        let paths = fresh_home("review-retired-progress");
        let state = Arc::new(Mutex::new(State::load(paths).unwrap()));
        let att = attach_listener(&state, "on job.started then stream limit 1", &[], "test", None, None, None).unwrap();
        let mut s = state.lock().unwrap();
        s.matched_through = s.log.head_seq().unwrap();
        ingest_job(&mut s, "job.started", "review-retired");
        s.follow_step(256);
        assert_eq!(drain_rx(&att.rx).len(), 1);
        assert!(s.matcher.get(&att.id).is_none());
        assert!(!s.applied_seqs.contains_key(&att.id), "retired listener leaked its applied progress entry");
    }

