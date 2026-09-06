    #[test]
    fn review_failed_timer_arm_does_not_advance_follower() {
        let paths = fresh_home("review-arm-cursor");
        let state = Arc::new(Mutex::new(State::load(paths).unwrap()));
        let att = attach_listener(&state, LISTEN_EXPECT, &[], "test", None, None, None).unwrap();
        let mut s = state.lock().unwrap();
        s.matched_through = s.log.head_seq().unwrap();
        let before = s.matched_through;
        ingest_job(&mut s, "job.started", "review-arm");
        s.fail_persist_timers = true;
        s.follow_step(256);
        assert_eq!(s.matched_through, before, "failed durable timer must not consume replay position");
        assert!(timer_origin_ids(&s, &att.id).is_empty());
    }

    #[test]
    fn review_failed_disarm_retry_clears_durable_timer() {
        let paths = fresh_home("review-disarm");
        let state = Arc::new(Mutex::new(State::load(paths).unwrap()));
        let att = attach_listener(&state, LISTEN_EXPECT, &[], "test", None, None, None).unwrap();
        let mut s = state.lock().unwrap();
        s.matched_through = s.log.head_seq().unwrap();
        ingest_job(&mut s, "job.started", "review-disarm");
        s.drain();
        assert_eq!(timer_origin_ids(&s, &att.id).len(), 1);
        ingest_job(&mut s, "job.finished", "review-disarm");
        let head = s.log.head_seq().unwrap();
        let completion = s.log.read(Pos::local(head), 1).unwrap().pop().unwrap().1;
        s.fail_persist_timers = true;
        assert!(s.disarm_listener_timers(&att.id, &completion).is_err());
        s.fail_persist_timers = false;
        s.disarm_listener_timers(&att.id, &completion).unwrap();
        let persisted: Vec<Timer> = serde_json::from_slice(&std::fs::read(s.paths.timers()).unwrap()).unwrap();
        assert!(persisted.is_empty(), "successful retry left the cancelled timer on disk");
    }

