#!/usr/bin/env python3


import typing
import abc


class DataProcessor(abc.ABC):
    def __init__(self) -> None:
        self.storage: list[tuple[int, str]] = []
        self.counter: int = 0

    @abc.abstractmethod
    def validate(self, data: typing.Any) -> bool:
        ...

    @abc.abstractmethod
    def ingest(self, data: typing.Any) -> None:
        ...

    def output(self) -> tuple[int, str]:
        return self.storage.pop(0)


class NumericProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, (int, float)):
            return True
        if isinstance(data, list):
            for item in data:
                if not isinstance(item, (int, float)):
                    return False
            return True
        return False

    def ingest(self, data: int | float | list[int | float]) -> None:
        if not self.validate(data):
            raise ValueError("Improper numeric data")
        if isinstance(data, (int, float)):
            self.storage.append((self.counter, str(data)))
            self.counter = self.counter + 1
            return
        if isinstance(data, list):
            for item in data:
                self.storage.append((self.counter, str(item)))
                self.counter = self.counter + 1


class TextProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, str):
            return True
        if isinstance(data, list):
            for item in data:
                if not isinstance(item, str):
                    return False
            return True
        return False

    def ingest(self, data: str | list[str]) -> None:
        if not self.validate(data):
            raise ValueError("Improper text data")
        if isinstance(data, str):
            self.storage.append((self.counter, data))
            self.counter = self.counter + 1
            return
        if isinstance(data, list):
            for item in data:
                self.storage.append((self.counter, item))
                self.counter = self.counter + 1


class LogProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, dict):
            for key, value in data.items():
                if not isinstance(key, str) or not isinstance(value, str):
                    return False
            return True
        if isinstance(data, list):
            for item in data:
                if not self.validate(item):
                    return False
            return True
        return False

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        if not self.validate(data):
            raise ValueError("Improper log data")
        if isinstance(data, dict):
            entry = f"{data['log_level']}: {data['log_message']}"
            self.storage.append((self.counter, entry))
            self.counter = self.counter + 1
            return
        if isinstance(data, list):
            for item in data:
                entry = f"{item['log_level']}: {item['log_message']}"
                self.storage.append((self.counter, entry))
                self.counter = self.counter + 1


class DataStream:
    def __init__(self) -> None:
        self.processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self.processors.append(proc)
        
    def process_stream(self, stream: list[typing.Any]) -> None:
        ...

    def print_processors_stats(self) -> None:
        ...


def main() -> None:
    print("=== Code Nexus - Data Processor ===\n")
    print("Testing Numeric Processor...")
    numeric = NumericProcessor()
    print(f" Trying to validate input '42': {numeric.validate(42)}")
    print(f" Trying to validate input 'Hello': {numeric.validate('Hello')}")
    print(" Test invalid ingestion of string 'foo' without prior validation:")
    try:
        numeric.ingest("foo")
    except ValueError as e:
        print(f" Got exception: {e}")
    numeric_data: list[int | float] = [1, 2, 3, 4, 5]
    print(f" Processing data: {numeric_data}")
    if numeric.validate(numeric_data):
        numeric.ingest(numeric_data)
        print(" Extracting 3 values...")
        for i in range(3):
            rank, value = numeric.output()
            print(f" Numeric value {rank}: {value}")
    print()

    print("Testing Text Processor...")
    text = TextProcessor()
    print(f" Trying to validate input '42': {text.validate(42)}")
    text_data = ['Hello', 'Nexus', 'World']
    print(f" Processing data: {text_data}")
    if text.validate(text_data):
        text.ingest(text_data)
        print(" Extracting 1 value...")
        rank, value = text.output()
        print(f" Text value {rank}: {value}")
    print()

    print("Testing Log Processor...")
    log = LogProcessor()
    print(f" Trying to validate input 'Hello': {log.validate('Hello')}")
    log_data = [{'log_level': 'NOTICE', 'log_message': 'Connection to server'},
                {'log_level': 'ERROR', 'log_message': 'Unauthorized access!!'}]
    print(f" Processing data: {log_data}")
    if log.validate(log_data):
        log.ingest(log_data)
        print(" Extracting 2 values...")
        for i in range(2):
            rank, log_message = log.output()
            print(f" Log entry {rank}: {log_message}")


if __name__ == "__main__":
    main()
