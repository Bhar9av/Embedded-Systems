import csv


class CSVReader:

    @staticmethod
    def read_signal_rules(filename="Signals.csv"):
        rules = {}
        with open(filename, newline="") as file:
            reader = csv.DictReader(file)
            for row in reader:
                rules[row["signal_name"]] = {
                    "min": float(row["min_value"]),
                    "max": float(row["max_value"]),
                    "delay": int(row["fault_delay_ms"])
                }
        return rules

    @staticmethod
    def read_logs(filename="Logs.csv"):
        logs = []
        with open(filename, newline="") as file:
            reader = csv.DictReader(file)
            for row in reader:
                logs.append({
                    "timestamp": int(row["timestamp_ms"]),
                    "signal": row["signal_name"],
                    "value": float(row["value"])
                })

        return logs


class SignalValidator:

    def __init__(self, name, min_value, max_value, delay):
        self.name = name
        self.min = min_value
        self.max = max_value
        self.delay = delay

        self.violation_start = None
        self.failed = False
        self.last_timestamp = None

    def process_sample(self, timestamp, value):

        if self.failed:
            return

        in_range = self.min <= value <= self.max

        if not in_range:
            if self.violation_start is None:
                self.violation_start = timestamp
            elif timestamp - self.violation_start >= self.delay:
                self.failed = True
        else:
            self.violation_start = None

        self.last_timestamp = timestamp

    def get_result(self):
        return "FAIL" if self.failed else "PASS"


class ValidationEngine:

    def __init__(self, signal_rule):
        self.validators = {}

        for name, rule in signal_rule.items():
            self.validators = SignalValidator(
                name,
                rule["min"],
                rule["max"],
                rule["delay"]
            )

    def run(self, logs):

        logs.sort(key=lambda x: x["timestamp"])

        for entry in logs:
            signal = entry["signals"]
            if signal in self.validators:
                self.validators[signal].process_sample(
                    entry["timestamp"],
                    entry['value']
                )

    def report(self):
        for name, validator in self.validators.items():
            print(f"{name}: {validator.get_result()}")


if __name__ == "__main__":
    rules = CSVReader.read_signal_rules("signals.csv")
    logs = CSVReader.read_logs("log.csv")

    engine = ValidationEngine(rules)
    engine.run(logs)
    engine.report()
