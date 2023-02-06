/*
A task set produces jobs for a given point in time.
Across scheduling we track the list of job specs as well as their status.

When scheduling a timeslice of unit length is considered at each iteration.
The priorization of jobs is defined by a PrioritizeFn (e.g. RMS, EDF) which puts the jobs
to be scheduled first. Then at most core count jobs are out into the current timeslice.
That is sufficient to implement global scheduling though it does not minizie context switches.

Partioning algorithms are defined by a PartitionFn, SortFn as well as a FitFn.
The SortFn defines how the task set is ordederd (e.g. by period or utilization) before
partioning. The PartioninFn dustributes a tasks set onto the cores (e.g. first fit or
next fit) using the FitFn (Liu-Layland or Hyperbolic Bound) to determine when a core is full.
*/

module main

import arrays
import math

struct Task {
	id     string
	period int
	exec   int
}

fn new_task(id string, period int, exec int) Task {
	return Task{id, period, exec}
}

fn (t Task) util() f32 {
	return f32(t.exec) / f32(t.period)
}

struct JobSpec {
	id       string
	period   int
	release  int
	deadline int
	exec     int
}

struct JobStatus {
mut:
	progress int
	util     f32
}

struct Job {
	spec JobSpec
mut:
	status JobStatus
}

const idle_job = Job{
	spec: JobSpec{
		id: '.'
	}
}

struct TaskSet {
	tasks []Task
}

fn (ts TaskSet) generate_jobs(period int) []Job {
	mut generated := []Job{}
	for task in ts.tasks {
		if period % task.period == 0 {
			j := Job{
				spec: JobSpec{
					id: task.id
					period: task.period
					release: period
					deadline: period + task.period
					exec: task.exec
				}
				status: JobStatus{
					progress: 0
					util: task.util()
				}
			}
			generated << j
		}
	}
	return generated
}

fn (ts TaskSet) hyperperiod() int {
	return match ts.tasks.len {
		0 {
			0
		}
		1 {
			ts.tasks[0].period
		}
		else {
			mut hp := ts.tasks[0].period
			for i in 1 .. ts.tasks.len {
				hp = int(math.lcm(hp, ts.tasks[i].period))
			}
			hp
		}
	}
}

fn (ts TaskSet) sum_util() f32 {
	mut acc := f32(0)
	for t in ts.tasks {
		acc += t.util()
	}
	return acc
}

fn (ts TaskSet) min_period() int {
	return arrays.min(ts.tasks.map(it.period)) or { 0 }
}

fn (ts TaskSet) valid() bool {
	return !ts.tasks.any(it.util() > 1.0)
}

// may produce non-simple periodic task sets due to
// periods being integers in this model
fn (ts TaskSet) specialization_operation() []TaskSet {
	min := ts.min_period()
	mut sets := [][]Task{}
	for pivot in ts.tasks {
		base := pivot.period / math.pow(2, math.ceil(math.log2(pivot.period / min)))
		mut set := []Task{}
		for t in ts.tasks {
			shortend := base * math.pow(2, math.floor(math.log2(t.period / base)))
			set << new_task(t.id, int(shortend), t.exec)
		}
		sets << set
	}
	task_sets := sets.map(TaskSet{it})
	return task_sets.filter(it.valid())
}

type PrioritizeFn = fn (mut []Job)

fn rate_monotonic_prioritizer(mut jobs []Job) {
	jobs.sort_with_compare(fn (a &Job, b &Job) int {
		if a.spec.period < b.spec.period {
			return -1
		}
		if a.spec.period > b.spec.period {
			return 1
		}
		return 0
	})
}

fn earliest_deadline_first_prioritizer(mut jobs []Job) {
	jobs.sort_with_compare(fn (a &Job, b &Job) int {
		if a.spec.deadline < b.spec.deadline {
			return -1
		}
		if a.spec.deadline > b.spec.deadline {
			return 1
		}
		return 0
	})
}

struct Timeslice {
	scheduled []Job
}

struct Missed {
	job Job
	on  int
}

struct Schedule {
	cores      int
	timeslices []Timeslice
	missed     []Missed
}

fn (s Schedule) valid() bool {
	return s.missed.len == 0
}

fn (s Schedule) format_schedule() string {
	mut text := ''
	for i in 0 .. s.cores {
		for slice in s.timeslices {
			if i < slice.scheduled.len {
				text += slice.scheduled[i].spec.id
			}
		}
		if i != s.cores - 1 {
			text += '\n'
		}
	}
	return text
}

fn (s Schedule) format_missed() string {
	if s.missed.len == 0 {
		return 'no deadlines missed.'
	}
	mut text := 'missed deadlines:\n'
	for i, missed in s.missed {
		text += '${missed.job.spec.id} on ${missed.on}'
		if i != s.missed.len - 1 {
			text += '\n'
		}
	}
	return text
}

fn (s Schedule) format() string {
	return '${s.format_schedule()}\n${s.format_missed()}'
}

struct GlobalScheduler {
	set         TaskSet
	cores       int
	prioritizer PrioritizeFn
}

fn (sched &GlobalScheduler) schedule(until int) Schedule {
	mut slices := []Timeslice{}
	mut missed := []Missed{}
	mut active := []Job{}
	for i in 0 .. until {
		slices << sched.step(mut active, i)
		// in i == 0 no deadline can be missed and we
		// need to check for missed deadlines in until
		missed << sched.remove_missed(mut active, i + 1)
	}
	return Schedule{sched.cores, slices, missed}
}

fn (sched &GlobalScheduler) step(mut active []Job, period int) Timeslice {
	// add jobs
	active << sched.set.generate_jobs(period)
	sched.prioritizer(mut active)
	// add core-count most prioritized to timeslice
	mut scheduled := []Job{}
	for i in 0 .. sched.cores {
		if i < active.len {
			scheduled << active[i]
			active[i].status.progress += 1
		} else {
			scheduled << idle_job
		}
	}
	// remove if progress == exec
	// no clue why active.filter causes a C builderror
	for i := 0; i < active.len; i++ {
		if active[i].spec.exec == active[i].status.progress {
			active.delete(i)
			i -= 1
		}
	}
	return Timeslice{scheduled}
}

// remove jobs that deadlines were reached
fn (sched &GlobalScheduler) remove_missed(mut active []Job, period int) []Missed {
	mut missed := []Missed{}
	for i := 0; i < active.len; i++ {
		if active[i].spec.deadline == period {
			missed << Missed{
				job: active[i]
				on: period
			}
			active.delete(i)
			i -= 1
		}
	}
	return missed
}

type FitFn = fn ([]Task, &Task) bool

fn fit_full(placed []Task, next &Task) bool {
	used := TaskSet{placed}.sum_util()
	return used + next.util() <= 1.0
}

fn fit_liu_layland(placed []Task, next &Task) bool {
	task_count := placed.len + 1
	bound := task_count * (math.pow(2, 1 / task_count) - 1)
	used := TaskSet{placed}.sum_util()
	return used + next.util() <= bound
}

fn fit_hyperbolic_bound(placed []Task, next &Task) bool {
	mut acc := 1 + next.util()
	for i in 0 .. placed.len {
		acc = acc * (1 + placed[i].util())
	}
	return acc <= 2
}

type PartitionFn = fn (&TaskSet, FitFn) []TaskSet

fn next_fit(set &TaskSet, fit FitFn) []TaskSet {
	mut cores := []TaskSet{}
	mut current := []Task{}
	for t in set.tasks {
		if fit(current, t) {
			current << t
			continue
		}
		cores << TaskSet{current.clone()}
		current.clear()
		current << t
	}
	if current.len > 0 {
		cores << TaskSet{current}
	}
	return cores
}

fn first_fit(set &TaskSet, fit FitFn) []TaskSet {
	mut cores := [][]Task{len: 0}
	for t in set.tasks {
		mut fits := false
		for mut c in cores {
			if fit(c, t) {
				c << t
				fits = true
			}
		}
		if fits {
			continue
		}
		cores << []Task{}
		cores.last() << t
	}
	return cores.map(TaskSet{it})
}

type SortFn = fn (mut []Task)

fn sort_identity(mut tasks []Task) {}

fn sort_period(mut tasks []Task) {
	tasks.sort_with_compare(fn (a &Task, b &Task) int {
		if a.period < b.period {
			return -1
		}
		if a.period > b.period {
			return 1
		}
		return 0
	})
}

fn sort_utilization(mut tasks []Task) {
	tasks.sort_with_compare(fn (a &Task, b &Task) int {
		if a.util() < b.util() {
			return -1
		}
		if a.util() > b.util() {
			return 1
		}
		return 0
	})
}

struct PartitionScheduler {
	set         TaskSet
	prioritizer PrioritizeFn
	partitioner PartitionFn
	fitter      FitFn
	sorter      SortFn
}

fn (sched &PartitionScheduler) schedule(until int) Schedule {
	mut sorted := sched.set.tasks.clone()
	sched.sorter(mut sorted)
	sets := sched.partitioner(&TaskSet{sorted}, sched.fitter)
	// make unicore schedules
	mut plans := []Schedule{}
	for set in sets {
		single_sched := GlobalScheduler{
			set: set
			cores: 1
			prioritizer: sched.prioritizer
		}
		plans << single_sched.schedule(until)
	}
	// merge unicore schedules
	mut slices := []Timeslice{}
	for i in 0 .. until {
		mut slice := []Job{}
		for core in 0 .. plans.len {
			slice << plans[core].timeslices[i].scheduled[0] or { idle_job }
		}
		slices << Timeslice{slice}
	}
	// merge missed deadlines
	mut missed := []Missed{}
	for plan in plans {
		missed << plan.missed
	}
	return Schedule{plans.len, slices, missed}
}

fn main() {
	set := TaskSet{
		tasks: [new_task('c', 6, 2), new_task('a', 4, 2), new_task('b', 3, 1)]
	}
	print('Global RMS ')
	sched_rms := GlobalScheduler{
		set: set
		cores: 2
		prioritizer: rate_monotonic_prioritizer
	}
	plan_rms := sched_rms.schedule(set.hyperperiod())
	println('valid: ${plan_rms.valid()}')
	println('${plan_rms.format()}')

	print('Global EDF ')
	sched_edf := GlobalScheduler{
		set: set
		cores: 2
		prioritizer: earliest_deadline_first_prioritizer
	}
	plan_edf := sched_edf.schedule(set.hyperperiod())
	println('valid: ${plan_edf.valid()}')
	println('${plan_edf.format()}')

	print('Part ')
	sched_part := PartitionScheduler{
		set: set
		partitioner: first_fit
		prioritizer: earliest_deadline_first_prioritizer
		fitter: fit_hyperbolic_bound
		sorter: sort_period
	}
	plan_part := sched_part.schedule(set.hyperperiod())
	println('valid: ${plan_part.valid()}')
	println('${plan_part.format()}')
}
