import os
import subprocess
import pyinotify
import sys

fname = sys.argv[1]
file_to_watch = f"winner_scores/{fname}.json"
script_to_run = "processFile.py"
script_arguments     = [fname]

class FileModifiedHandler(pyinotify.ProcessEvent):
    def process_default(self, event):
        if event.pathname == os.path.abspath(file_to_watch):
            print(f"\nFile {file_to_watch} has been modified. Running {script_to_run} with arguments {script_arguments}.")
            subprocess.run(["python3", script_to_run] + script_arguments)

def main():
    wm = pyinotify.WatchManager()
    notifier = pyinotify.Notifier(wm, FileModifiedHandler())

    wm.add_watch(file_to_watch, pyinotify.IN_MODIFY)

    try:
        print(f"Watching {file_to_watch} for changes...")
        notifier.loop()
    except KeyboardInterrupt:
        notifier.stop()
        print(f"\nMonitoring stopped.")

if __name__ == "__main__":
    main()