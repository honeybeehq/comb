    #[test]
    fn review_timer_retry_does_not_repeat_successful_listener() {
        let paths = fresh_home("review-partial-cascade");
        let state = Arc::new(Mutex::new(State::load(paths).unwrap()));
        let plain = attach_listener(&state, "on job.started then stream limit 2", &[], "test", None, None, None).unwrap();
        let expect = attach_listener(&state, LISTEN_EXPECT, &[], "test", None, None, None).unwrap();
        let mut s = state.lock().unwrap();
        s.matched_through = s.log.head_seq().unwrap();
        let before = s.matched_through;
        ingest_job(&mut s, "job.started", "review-partial");
        let head = s.log.head_seq().unwrap();
        let origin = s.log.read(Pos::local(head), 1).unwrap().pop().unwrap().1;
        let order = s.matcher.match_ids(&origin);
        assert_eq!(order, vec![plain.id.as_str(), expect.id.as_str()], "fixture must deliver before failed timer arm");
        s.fail_persist_timers = true;
        s.follow_step(256);
        assert_eq!(s.matched_through, before);
        assert_eq!(drain_rx(&plain.rx), vec![origin.id.clone()]);
        s.fail_persist_timers = false;
        s.follow_step(256);
        assert_eq!(timer_origin_ids(&s, &expect.id).len(), 1);
        assert!(drain_rx(&plain.rx).is_empty(), "retry repeated delivery to an already successful listener");
        assert!(s.matcher.get(&plain.id).is_some(), "retry consumed the listener limit twice");
    }
