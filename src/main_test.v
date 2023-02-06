module main

fn test_edf() {
	set := TaskSet{
		tasks: [new_task('a', 3, 1), new_task('b', 5, 2), new_task('c', 8, 3)]
	}
	scheduler := GlobalScheduler{
		set: set
		cores: 1
		prioritizer: earliest_deadline_first_prioritizer
	}
	sched := scheduler.schedule(set.hyperperiod())
	assert sched.missed[0].job.spec.id == 'c'
	assert sched.missed[0].on == 16
	assert sched.missed[1].job.spec.id == 'b'
	assert sched.missed[1].on == 25
}

fn test_rms() {
	set := TaskSet{
		tasks: [new_task('a', 3, 1), new_task('b', 5, 2), new_task('c', 8, 3)]
	}
	scheduler := GlobalScheduler{
		set: set
		cores: 1
		prioritizer: rate_monotonic_prioritizer
	}
	sched := scheduler.schedule(set.hyperperiod())
	assert sched.missed[0].job.spec.id == 'c'
	assert sched.missed[0].on == 8
	assert sched.missed[1].job.spec.id == 'c'
	assert sched.missed[1].on == 24
}

fn test_specialization_operation() {
	set := TaskSet{
		tasks: [new_task('a', 6, 1), new_task('b', 8, 3), new_task('c', 10, 2),
			new_task('d', 24, 1), new_task('e', 49, 1)]
	}
	sets := set.specialization_operation()
	assert sets.len == 5
	assert sets[0].tasks.map(it.period) == [6, 6, 6, 24, 48]
	assert sets[1].tasks.map(it.period) == [4, 8, 8, 16, 32]
	assert sets[2].tasks.map(it.period) == [5, 5, 10, 20, 40]
	assert sets[3].tasks.map(it.period) == [6, 6, 6, 24, 48]
	assert sets[4].tasks.map(it.period) == [3, 6, 6, 12, 49]
}
