# frontend.py

import tkinter as tk
from tkinter import messagebox
import time
import threading
import queue
import processing_excel  # Make sure processing_excel.py is in the same folder

start_time = 0
timer_running = False

def start():
    global timer_running, start_time
    processing_excel.start_watcher()
    start_time = time.time()
    timer_running = True
    status_label.config(text="Status: Watching", fg="green")
    update_timer()

def stop():
    global timer_running
    processing_excel.stop_watcher()
    processing_excel.clear_queue(processing_excel.file_queue)
    processing_excel.clear_queue(processing_excel.preprocessing_queue)
    timer_running = False
    timer_label.config(text="Time: 00:00:00")
    status_label.config(text="Status: Stopped", fg="red")
    log("\n--- STOPPED ---\n")

def update_timer():
    if timer_running:
        elapsed = int(time.time() - start_time)
        h, m, s = elapsed // 3600, (elapsed % 3600) // 60, elapsed % 60
        timer_label.config(text=f"Time: {h:02d}:{m:02d}:{s:02d}")
        root.after(1000, update_timer)

def poll_logs():
    try:
        while True:
            msg = processing_excel.get_log_queue().get_nowait()
            log(msg)
    except queue.Empty:
        pass
    root.after(500, poll_logs)

def log(message):
    log_text.config(state=tk.NORMAL)
    log_text.insert(tk.END, message + '\n')
    log_text.see(tk.END)
    log_text.config(state=tk.DISABLED)

# GUI Setup
root = tk.Tk()
root.title("Excel Processor")

frame = tk.Frame(root, padx=20, pady=10)
frame.pack()

start_btn = tk.Button(frame, text="Start", command=start, bg="green", fg="white", width=10)
start_btn.grid(row=0, column=0, padx=10)

stop_btn = tk.Button(frame, text="Stop", command=stop, bg="red", fg="white", width=10)
stop_btn.grid(row=0, column=1, padx=10)

status_label = tk.Label(frame, text="Status: Stopped", fg="red", font=("Arial", 12))
status_label.grid(row=1, column=0, columnspan=2, pady=10)

timer_label = tk.Label(frame, text="Time: 00:00:00", font=("Courier", 14))
timer_label.grid(row=2, column=0, columnspan=2, pady=5)

log_frame = tk.Frame(root)
log_frame.pack(padx=10, pady=10)

log_text = tk.Text(log_frame, height=20, width=80, state=tk.DISABLED, wrap=tk.WORD)
log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

scrollbar = tk.Scrollbar(log_frame, command=log_text.yview)
scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
log_text.config(yscrollcommand=scrollbar.set)

root.protocol("WM_DELETE_WINDOW", lambda: (stop(), root.destroy()))
poll_logs()
root.mainloop()
  

  