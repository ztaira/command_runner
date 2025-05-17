"""Simple program to run commands on a timer"""

from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Tuple, Optional
import time
import csv
import argparse
import multiprocessing
import subprocess
from dataclasses import dataclass, field, asdict, fields
import tomllib

DEFAULT_CONFIG_LOCATION = "/etc/command_runner/command_runner.toml"
DEFAULT_TASKS_LOCATION = "/etc/command_runner/tasks.csv"
DATES = ["mo", "tu", "we", "th", "fr", "sa", "su"]


@dataclass
class Days:
    """Class to store which days a command should be active

    >>> inactive = Days('--------------')
    >>> inactive.is_active_on(0)
    False
    >>> active = Days('mo--we--frsasu')
    >>> active.is_active_on(0)
    True
    >>> active.is_active_on(1)
    False
    """

    date_string: str
    active: Tuple = field(init=False)

    def __post_init__(self):
        temp = [self.date_string[y : y + 2] for y in range(0, len(self.date_string), 2)]
        self.active = [
            temp[index] == DATES[index]  # pylint: disable=unnecessary-list-index-lookup
            for index, _ in enumerate(temp)
        ]

    def is_active_on(self, day: int):
        return self.active[day]


@dataclass
class Task:
    """Class to store a task's info

    >>> all_day = Task(command='ls', owner='zt', days='motuwethfrsasu', time_start='00:00', time_end='23:59', reload_time='06:00')
    >>> all_day.is_active_now()
    True
    >>> midnight = Task(command='ls', owner='zt', days='motuwethfrsasu', time_start='00:00', time_end='00:01', reload_time='06:00')
    >>> midnight.is_active_now()
    False
    """

    command: str
    owner: str
    days: str
    time_start: str
    time_end: str
    reload_time: str
    last_execution: Optional[datetime] = field(init=False)
    day_bools: Optional[Tuple] = field(init=False)

    def __post_init__(self):
        self.last_execution = None
        self.day_bools = Days(self.days)

    @property
    def reload_timedelta(self):
        reload_hours, _, reload_min = self.reload_time.partition(":")
        return timedelta(hours=int(reload_hours), minutes=int(reload_min))

    def __eq__(self, other):
        return (
            self.command == other.command
            and self.owner == other.owner
            and self.days == other.days
            and self.time_start == other.time_start
            and self.time_end == other.time_end
            and self.reload_time == other.reload_time
        )

    def __str__(self):
        return (
            f"<({self.command}) from {self.owner}, executed on {self.last_execution}>"
        )

    @classmethod
    def from_dict(cls, row: Dict):
        return cls(**row)

    @classmethod
    def keys(cls):
        return_obj = [fld.name for fld in fields(cls)]
        return_obj.remove("last_execution")
        return_obj.remove("day_bools")
        return return_obj

    def is_active_now(self) -> bool:
        current_time = datetime.now()
        interval_start_time = datetime.combine(
            datetime.now(),
            datetime(*time.strptime(self.time_start, "%H:%M")[:6]).time(),
        )
        interval_end_time = datetime.combine(
            datetime.now(), datetime(*time.strptime(self.time_end, "%H:%M")[:6]).time()
        )
        if self.last_execution is not None:
            outside_reload_time = (
                datetime.now() - self.last_execution > self.reload_timedelta
            )
        else:
            outside_reload_time = True

        if (
            self.day_bools.is_active_on(current_time.weekday())
            and interval_start_time < datetime.now() < interval_end_time
            and outside_reload_time
        ):
            return True
        return False


@dataclass
class Runner:
    config_file: str = DEFAULT_CONFIG_LOCATION
    tasks_file: str = DEFAULT_TASKS_LOCATION
    tasks: List[Task] = field(default_factory=list)
    config: Dict = field(default_factory=dict)

    def __post_init__(self):
        self.load_config()
        self.load_tasks()

    def load_config(self):
        with Path(self.config_file).expanduser().open("rb") as readfile:
            self.config = tomllib.load(readfile)["command_runner"]

    def load_tasks(self):
        with Path(self.tasks_file).expanduser().open("r", encoding="utf-8") as readfile:
            csvreader = csv.DictReader(readfile)
            for row in csvreader:
                self.tasks.append(Task.from_dict(row))

    def run(self):
        print("Tasks:")
        for task in self.tasks:
            print(task)

        while True:
            tasks = [task for task in self.tasks if task.is_active_now()]
            for task in tasks:
                print(f"Active task: {task}")
            with multiprocessing.Pool(4) as pool:
                updated_tasks = pool.map(run_task, tasks)
            for task in updated_tasks:
                self.tasks.remove(task)
                self.tasks.append(task)
            for task in self.tasks:
                print(task)
            print("\n\n")
            time.sleep(self.config["check_interval"])
            tasks = []


def run_task(task: Task):
    print(f"Executing task: {task}")
    result = subprocess.run(task.command, capture_output=True, shell=True, check=False)
    print(f"Result of command {task.command} was: {result}")
    task.last_execution = datetime.now()
    return task


def create_task(data_file: str):
    if not Path(data_file).expanduser().exists():
        with Path(data_file).expanduser().open("w", encoding="utf-8") as writefile:
            csvwriter = csv.DictWriter(writefile, Task.keys())
            csvwriter.writeheader()

    with Path(data_file).expanduser().open("a+", encoding="utf-8") as writefile:
        task = Task(
            command=input("Command to run: "),
            owner=input("Command owner: "),
            days=input("Days of the week it should execute: "),
            time_start=input("Start of execution window: "),
            time_end=input("End of execution window: "),
            reload_time=input("Time before second execution: "),
        )
        csvwriter = csv.DictWriter(writefile, Task.keys())
        row = asdict(task)
        del row["last_execution"]
        del row["day_bools"]
        csvwriter.writerow(row)


def run_tasks(tasks_file: str, config_file: str):
    runner = Runner(config_file=config_file, tasks_file=tasks_file)
    runner.run()


def parse_args():
    main_parser = argparse.ArgumentParser(description="command_runner")
    main_parser.add_argument("-V", "--version", action="version", version="0.0.0")
    main_parser.set_defaults(run=lambda: print("Use a subparser!"))
    modes = main_parser.add_subparsers(title="Mode", metavar="")

    run_tasks_parser = modes.add_parser("run", help="Run tasks")
    run_tasks_parser.add_argument(
        "--tasks_file", type=str, default=DEFAULT_TASKS_LOCATION
    )
    run_tasks_parser.add_argument(
        "--config_file", type=str, default=DEFAULT_CONFIG_LOCATION
    )
    run_tasks_parser.set_defaults(run=run_tasks)

    create_task_parser = modes.add_parser("create_task", help="Create tasks")
    create_task_parser.add_argument(
        "--tasks_file", type=str, default=DEFAULT_TASKS_LOCATION
    )
    create_task_parser.set_defaults(run=create_task)

    return main_parser.parse_args()


def main():
    args = parse_args()
    args.run(**{key: value for key, value in vars(args).items() if key != "run"})


if __name__ == "__main__":
    main()
