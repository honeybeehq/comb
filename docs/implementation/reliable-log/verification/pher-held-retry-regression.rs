    #[test]
    fn review_timer_retry_does_not_duplicate_held_live() {
        let paths = fresh_home("review-held-retry");
        let state = Arc::new(Mutex::new(State::load(paths).unwrap()));
        let held = attach_listener(&state, "on job.started then stream limit 2", &[], "test", Some(0), None, None).unwrap();
        let expect = attach_listener(&state, LISTEN_EXPECT, &[], "test", None, None, None).unwrap();
        let mut s = state.lock().unwrap();
        s.matched_through = s.log.head_seq().unwrap();
        let before = s.matched_through;
        let origin = ingest_job(&mut s, "job.started", "review-held");
        s.fail_persist_timers = true;
        s.follow_step(256);
        assert_eq!(s.matched_through, before);
        s.fail_persist_timers = false;
        s.follow_step(256);
        assert_eq!(timer_origin_ids(&s, &expect.id).len(), 1);
        s.release_listen_catchup(&held.id).unwrap();
        assert_eq!(drain_rx(&held.rx), vec![origin], "retry queued duplicate live events during catch-up");
        assert!(s.matcher.get(&held.id).is_some(), "duplicate held event consumed limit twice");
    }
