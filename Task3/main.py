import json
import time
import threading
from abc import ABC, abstractmethod
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor


# ============================================================
#                  OBSERVER PATTERN
# ============================================================

class Observer(ABC):
    @abstractmethod
    def update(self, task, old_status, new_status):
        pass


class TaskLogger(Observer):
    """Logs task status changes."""

    def update(self, task, old_status, new_status):
        print(f"[LOG] Task {task.task_id} ({task.name}) status changed: {old_status} → {new_status}")


# ============================================================
#                     BASE TASK CLASS
# ============================================================

class Task(ABC):
    def __init__(self, task_id, name):
        self.task_id = task_id
        self.name = name
        self.status = "Pending"
        self.created_at = datetime.now()

        self._observers = []

    # Attach / detach observers
    def attach(self, observer):
        self._observers.append(observer)

    def notify(self, old_status, new_status):
        for obs in self._observers:
            obs.update(self, old_status, new_status)

    def set_status(self, new_status):
        old = self.status
        self.status = new_status
        self.notify(old, new_status)

    @abstractmethod
    def run(self):
        pass


# ============================================================
#                    TASK SUBCLASSES
# ============================================================

class EmailTask(Task):
    def run(self):
        try:
            self.set_status("Running")
            time.sleep(2)  # Simulate work
            print(f"[EmailTask] Sending email for Task {self.task_id}")
            self.set_status("Completed")
        except Exception:
            self.set_status("Failed")


class DataBackupTask(Task):
    def run(self):
        try:
            self.set_status("Running")
            time.sleep(3)
            print(f"[DataBackupTask] Backing up data for Task {self.task_id}")
            self.set_status("Completed")
        except Exception:
            self.set_status("Failed")


class ReportGenerationTask(Task):
    def run(self):
        try:
            self.set_status("Running")
            time.sleep(1)
            print(f"[ReportGenerationTask] Generating report for Task {self.task_id}")
            self.set_status("Completed")
        except Exception:
            self.set_status("Failed")


# ============================================================
#                  FACTORY PATTERN
# ============================================================

class TaskFactory:
    @staticmethod
    def create_task(task_json):
        task_type = task_json["type"]
        task_id = task_json["task_id"]
        name = task_json["name"]

        if task_type == "email":
            return EmailTask(task_id, name)

        if task_type == "backup":
            return DataBackupTask(task_id, name)

        if task_type == "report":
            return ReportGenerationTask(task_id, name)

        raise ValueError(f"Unknown task type: {task_type}")


# ============================================================
#              TASK SCHEDULER (with concurrency)
# ============================================================

class TaskScheduler:
    def __init__(self):
        self.tasks = []
        self.logger = TaskLogger()

    def add_task(self, task):
        task.attach(self.logger)
        self.tasks.append(task)

    def run_all(self):
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(task.run) for task in self.tasks]
            for f in futures:
                f.result()  # Wait for all tasks


# ============================================================
#                        DEMO
# ============================================================

if __name__ == "__main__":
    # Sample task definitions (could come from API / JSON file)
    json_input = """
    [
        {"task_id": 1, "name": "Send Welcome Email", "type": "email"},
        {"task_id": 2, "name": "Daily DB Backup", "type": "backup"},
        {"task_id": 3, "name": "Sales Report", "type": "report"},
        {"task_id": 4, "name": "Another Email", "type": "email"}
    ]
    """

    task_data = json.loads(json_input)

    scheduler = TaskScheduler()

    # Create tasks via factory
    for data in task_data:
        task = TaskFactory.create_task(data)
        scheduler.add_task(task)

    print("=== Starting Task Scheduler ===")
    scheduler.run_all()
    print("=== All Tasks Finished ===")
