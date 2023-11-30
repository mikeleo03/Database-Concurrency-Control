class FileHandler:
    def __init__(self, filename):
        self.file = open(filename, 'r')
        self.lines = []
        for line in self.file:
            self.lines.append(line.strip(";\n").strip())
        self.index = 0
        self.max_lines = len(self.lines)

    def next_line(self):
        if self.index >= self.max_lines:
            return ''

        str = self.lines[self.index]
        self.index += 1

        return str

    def __del__(self):
        self.file.close()
