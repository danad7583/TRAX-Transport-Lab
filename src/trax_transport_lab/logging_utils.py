class DemoLog:
    def __init__(self):
        self.lines: list[str] = []

    def add(self, line: str) -> None:
        self.lines.append(line)

    def reject(self, message_type: str, reason: str, dag_append: bool = False) -> None:
        self.add(
            f"rejected message_type={message_type} reason={reason} dag_append={dag_append}"
        )
