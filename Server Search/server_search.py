import os
import tkinter as tk
from tkinter import filedialog, messagebox, Scrollbar, ttk
import subprocess
import platform
from queue import Queue, Empty
import threading
import time
import json
from datetime import datetime
from pathlib import Path
import hashlib

class FileIndex:
    def __init__(self):
        self.index = {}
        self.last_update = None
        self.indexing = False
        self.index_file = "file_index.json"
        self.load_index()
        
    def load_index(self):
        try:
            if os.path.exists(self.index_file):
                with open(self.index_file, 'r') as f:
                    data = json.load(f)
                    self.index = data.get('files', {})
                    last_update = data.get('last_update')
                    if last_update:
                        self.last_update = datetime.fromisoformat(last_update)
        except Exception as e:
            print(f"Error loading index: {e}")
            self.index = {}
            self.last_update = None
    
    def save_index(self):
        try:
            with open(self.index_file, 'w') as f:
                json.dump({
                    'last_update': self.last_update.isoformat() if self.last_update else None,
                    'files': self.index
                }, f)
        except Exception as e:
            print(f"Error saving index: {e}")
    
    def update_index(self, base_dir, progress_callback=None):
        self.indexing = True
        try:
            new_index = {}
            processed = 0
            scanned = 0
            total_files = 0
            
            # Use a list to collect all files first
            all_files = []
            for root, _, files in os.walk(base_dir):
                for file in files:
                    if not self.indexing:  # Check if indexing was cancelled
                        return
                    filepath = os.path.normpath(os.path.join(root, file))
                    all_files.append(filepath)
                    scanned += 1
                    # Emit occasional progress during directory scan (before total is known)
                    if progress_callback and scanned % 1000 == 0:
                        progress_callback(-1, scanned, 0)
            
            total_files = len(all_files)
            print(f"Found {total_files} files to index")
            
            # Now process each file
            for filepath in all_files:
                if not self.indexing:  # Check if indexing was cancelled
                    return
                
                try:
                    stat = os.stat(filepath)
                    filename = os.path.basename(filepath)
                    new_index[filepath] = {
                        'name': filename.lower(),
                        'path': filepath,
                        'parent': os.path.dirname(filepath),
                        'size': stat.st_size,
                        'modified': stat.st_mtime
                    }
                except Exception as e:
                    print(f"Error indexing {filepath}: {e}")
                    continue
                
                processed += 1
                if progress_callback and processed % 100 == 0:  # Update every 100 files
                    progress = (processed / total_files) * 100
                    progress_callback(progress, processed, total_files)
            
            # Final progress update
            if progress_callback:
                progress_callback(100, total_files, total_files)
            
            self.index = new_index
            self.last_update = datetime.now()
            self.save_index()
            print(f"Indexing complete. Added {len(new_index)} files to index")
        finally:
            self.indexing = False
    
    def search(self, keyword):
        """Search for files matching the keyword"""
        keyword = keyword.lower()
        results = []
        
        for info in self.index.values():
            if not info:  # Skip any corrupted entries
                continue
            try:
                filename = info['name']
                if keyword in filename:
                    results.append(info)
            except Exception as e:
                print(f"Error searching entry: {e}")
                continue
        
        # Sort results by filename
        results.sort(key=lambda x: x['name'])
        print(f"Found {len(results)} matches for '{keyword}'")
        return results

    def _partial_hash(self, path, nbytes=256 * 1024):
        h = hashlib.sha256()
        try:
            with open(path, 'rb') as f:
                chunk = f.read(nbytes)
                h.update(chunk)
            return h.hexdigest()
        except Exception as e:
            print(f"Partial hash error for {path}: {e}")
            return None

    def _full_hash(self, path, chunk_size=1024 * 1024):
        h = hashlib.sha256()
        try:
            with open(path, 'rb') as f:
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    h.update(chunk)
            return h.hexdigest()
        except Exception as e:
            print(f"Full hash error for {path}: {e}")
            return None

    def find_duplicates(self, min_size_bytes=1 * 1024 * 1024, progress_callback=None, stop_event=None, base_dir=None):
        # Step 1: group by size (optionally limited to a base directory)
        size_groups = {}
        if base_dir:
            base_dir_norm = os.path.normpath(base_dir)
            items = [info for info in self.index.values()
                     if info and os.path.normpath(info.get('path', '')).startswith(base_dir_norm)]
        else:
            items = [info for info in self.index.values() if info]
        total = len(items)
        for i, info in enumerate(items, 1):
            if stop_event and stop_event.is_set():
                return []
            try:
                size = info.get('size', 0)
                if size >= min_size_bytes:
                    size_groups.setdefault(size, []).append(info)
            except Exception:
                continue
            if progress_callback and i % 1000 == 0:
                progress_callback('Scanning by size', i, total)

        # Step 2: partial hash within same-size groups
        partial_groups = {}
        processed = 0
        candidates = sum(len(v) for v in size_groups.values() if len(v) > 1)
        for size, group in size_groups.items():
            if len(group) <= 1:
                continue
            for info in group:
                if stop_event and stop_event.is_set():
                    return []
                ph = self._partial_hash(info['path'])
                key = (size, ph)
                partial_groups.setdefault(key, []).append(info)
                processed += 1
                if progress_callback and processed % 200 == 0:
                    progress_callback('Partial hashing', processed, candidates)

        # Step 3: full hash within partial-collided groups
        full_groups = {}
        processed_full = 0
        candidates_full = sum(len(v) for k, v in partial_groups.items() if k[1] and len(v) > 1)
        for (size, ph), group in partial_groups.items():
            if not ph or len(group) <= 1:
                continue
            for info in group:
                if stop_event and stop_event.is_set():
                    return []
                fh = self._full_hash(info['path'])
                if not fh:
                    continue
                full_groups.setdefault(fh, []).append(info)
                processed_full += 1
                if progress_callback and processed_full % 50 == 0:
                    progress_callback('Full hashing', processed_full, candidates_full)

        # Build duplicate sets (hash -> list of files with count > 1)
        duplicate_sets = []
        for content_hash, group in full_groups.items():
            if len(group) > 1:
                duplicate_sets.append({
                    'hash': content_hash,
                    'count': len(group),
                    'size': group[0]['size'] if group else 0,
                    'files': group,
                })
        duplicate_sets.sort(key=lambda g: (-g['count'], -g['size']))
        return duplicate_sets
    
    def cancel_indexing(self):
        self.indexing = False

class IndexingThread(threading.Thread):
    def __init__(self, base_dir, file_index, queue, stop_event):
        super().__init__()
        self.base_dir = base_dir
        self.file_index = file_index
        self.queue = queue
        self.stop_event = stop_event
    
    def run(self):
        try:
            self.file_index.update_index(self.base_dir, self.update_progress)
        except Exception as e:
            self.queue.put(('error', str(e)))
        finally:
            self.queue.put(('index_complete', None))
    
    def update_progress(self, progress, processed, total):
        self.queue.put(('index_progress', (progress, processed, total)))

class SearchThread(threading.Thread):
    def __init__(self, keyword, file_index, queue, stop_event):
        super().__init__()
        self.keyword = keyword
        self.file_index = file_index
        self.queue = queue
        self.stop_event = stop_event
    
    def run(self):
        try:
            matches = self.file_index.search(self.keyword)
            total = len(matches)
            print(f"Processing {total} matches")
            
            for i, file_info in enumerate(matches, 1):
                if self.stop_event.is_set():
                    return
                
                self.queue.put(('result', file_info))
                
                # Update progress every 10 files or for the last file
                if i % 10 == 0 or i == total:
                    self.queue.put(('progress', (i, total)))
            
            self.queue.put(('done', None))
        except Exception as e:
            print(f"Search error: {e}")
            self.queue.put(('error', str(e)))

class DuplicateThread(threading.Thread):
    def __init__(self, file_index, queue, stop_event, min_size_bytes=1 * 1024 * 1024, base_dir=None):
        super().__init__()
        self.file_index = file_index
        self.queue = queue
        self.stop_event = stop_event
        self.min_size_bytes = min_size_bytes
        self.base_dir = base_dir

    def run(self):
        try:
            def progress(stage, current, total):
                self.queue.put(('dup_progress', (stage, current, total)))

            dup_sets = self.file_index.find_duplicates(
                min_size_bytes=self.min_size_bytes,
                progress_callback=progress,
                stop_event=self.stop_event,
                base_dir=self.base_dir
            )
            # Stream groups to UI
            for group in dup_sets:
                if self.stop_event.is_set():
                    return
                self.queue.put(('dup_group', group))
            self.queue.put(('dup_done', None))
        except Exception as e:
            self.queue.put(('error', str(e)))

def format_size(size):
    # Convert size to human readable format
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"

def format_date(timestamp):
    # Convert timestamp to readable date
    return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M')

def browse_directory():
    path = filedialog.askdirectory()
    if path:
        directory_path.set(path)
        start_indexing(path)

def start_indexing(path):
    global index_thread, stop_event
    
    if not os.path.isdir(path):
        messagebox.showerror("Invalid Path", "Please select a valid directory.")
        return
    
    # Update UI state
    search_button.config(state=tk.DISABLED)
    status_frame.pack(pady=10, fill=tk.X, padx=10)
    status_label.pack(fill=tk.X, padx=5)
    progress_label.pack(fill=tk.X, padx=5)
    progress_bar.pack(fill=tk.X, padx=5, pady=(0, 5))
    progress_bar['value'] = 0
    status_label.config(text="Indexing files...", fg="blue")
    progress_label.config(text="Starting indexing...")
    
    print(f"Starting indexing of directory: {path}")
    
    # Create a queue for communication between threads
    result_queue = Queue()
    stop_event = threading.Event()
    
    # Start indexing thread
    index_thread = IndexingThread(path, file_index, result_queue, stop_event)
    index_thread.start()
    
    # Start updating progress
    update_indexing_progress(result_queue)

def update_indexing_progress(queue):
    try:
        msg_type, data = queue.get_nowait()
        if msg_type == 'index_progress':
            progress, processed, total = data
            if progress < 0 or total == 0:
                # Scanning directory tree, total not known yet
                progress_label.config(text=f"Scanning directories... Files discovered: {processed:,}")
                print(f"Index scanning... files discovered: {processed}")
                progress_bar.config(mode="indeterminate")
                if not str(progress_bar.cget('mode')) == 'indeterminate':
                    progress_bar.config(mode="indeterminate")
                if not getattr(progress_bar, '_running', False):
                    progress_bar.start(50)
                    progress_bar._running = True
            else:
                # Real percentage-based progress
                if str(progress_bar.cget('mode')) != 'determinate':
                    progress_bar.stop()
                    progress_bar._running = False
                    progress_bar.config(mode="determinate")
                progress_label.config(text=f"Indexed: {processed:,} of {total:,} files ({progress:.1f}%)")
                print(f"Indexing progress: {progress:.1f}% ({processed}/{total} files)")
                try:
                    progress_bar['value'] = progress
                except Exception:
                    pass
        elif msg_type == 'index_complete':
            search_button.config(state=tk.NORMAL)
            status_label.config(text="Indexing complete - Ready to search", fg="green")
            progress_label.config(text="")
            try:
                progress_bar.stop()
            except Exception:
                pass
            progress_bar._running = False
            progress_bar.config(mode="determinate")
            progress_bar['value'] = 100
            progress_bar.pack_forget()
            print(f"Indexing complete. Total files in index: {len(file_index.index)}")
            return
        elif msg_type == 'error':
            messagebox.showerror("Error", f"An error occurred during indexing: {data}")
            search_button.config(state=tk.NORMAL)
            status_label.config(text="Indexing failed", fg="red")
            progress_label.config(text="")
            try:
                progress_bar.stop()
            except Exception:
                pass
            progress_bar._running = False
            progress_bar.config(mode="determinate")
            progress_bar['value'] = 0
            progress_bar.pack_forget()
            print(f"Indexing error: {data}")
            return
    except Empty:
        pass
    
    if index_thread and index_thread.is_alive():
        root.after(100, lambda: update_indexing_progress(queue))
    else:
        search_button.config(state=tk.NORMAL)

def start_search():
    global search_results, search_thread, stop_event
    keyword = keyword_entry.get().strip()
    base_dir = directory_path.get().strip()
    
    if not keyword:
        messagebox.showerror("Missing Input", "Please enter a keyword.")
        return
    
    if not base_dir or not os.path.isdir(base_dir):
        messagebox.showerror("Invalid Path", "Please select a valid directory first.")
        return
    
    # Determine if we need to index this directory first
    norm_base = os.path.normpath(base_dir)
    has_paths = any(p.startswith(norm_base) for p in file_index.index.keys())
    if not has_paths:
        start_indexing_then_search(norm_base, keyword)
        return
    
    do_start_search(keyword)

def do_start_search(keyword):
    global search_results, search_thread, stop_event
    print(f"Starting search for keyword: {keyword}")
    print(f"Current index size: {len(file_index.index)} files")
    
    for item in results_tree.get_children():
        results_tree.delete(item)
    search_results = []
    
    search_button.config(state=tk.DISABLED)
    cancel_button.config(state=tk.NORMAL)
    status_frame.pack(pady=10, fill=tk.X, padx=10)
    status_label.pack(fill=tk.X, padx=5)
    progress_label.pack(fill=tk.X, padx=5)
    status_label.config(text="Searching...", fg="blue")
    progress_label.config(text="Starting search...")
    
    result_queue = Queue()
    stop_event = threading.Event()
    search_thread = SearchThread(keyword, file_index, result_queue, stop_event)
    search_thread.start()
    update_results(result_queue)

def start_indexing_then_search(path, keyword):
    global index_thread, stop_event
    search_button.config(state=tk.DISABLED)
    if 'dup_button' in globals():
        dup_button.config(state=tk.DISABLED)
    status_frame.pack(pady=10, fill=tk.X, padx=10)
    status_label.pack(fill=tk.X, padx=5)
    progress_label.pack(fill=tk.X, padx=5)
    progress_bar.pack(fill=tk.X, padx=5, pady=(0, 5))
    progress_bar['value'] = 0
    status_label.config(text="Indexing files before search...", fg="blue")
    progress_label.config(text="Starting indexing...")
    
    result_queue = Queue()
    stop_event = threading.Event()
    index_thread = IndexingThread(path, file_index, result_queue, stop_event)
    index_thread.start()
    update_indexing_then_search(result_queue, keyword)

def update_indexing_then_search(queue, keyword):
    try:
        msg_type, data = queue.get_nowait()
        if msg_type == 'index_progress':
            progress, processed, total = data
            if progress < 0 or total == 0:
                progress_label.config(text=f"Scanning directories... Files discovered: {processed:,}")
                progress_bar.config(mode="indeterminate")
                if not getattr(progress_bar, '_running', False):
                    progress_bar.start(50)
                    progress_bar._running = True
            else:
                if str(progress_bar.cget('mode')) != 'determinate':
                    progress_bar.stop()
                    progress_bar._running = False
                    progress_bar.config(mode="determinate")
                progress_label.config(text=f"Indexed: {processed:,} of {total:,} files ({progress:.1f}%)")
                try:
                    progress_bar['value'] = progress
                except Exception:
                    pass
        elif msg_type == 'index_complete':
            progress_label.config(text="")
            try:
                progress_bar.stop()
            except Exception:
                pass
            progress_bar._running = False
            progress_bar.config(mode="determinate")
            progress_bar['value'] = 100
            progress_bar.pack_forget()
            do_start_search(keyword)
            return
        elif msg_type == 'error':
            messagebox.showerror("Error", f"An error occurred during indexing: {data}")
            search_button.config(state=tk.NORMAL)
            if 'dup_button' in globals():
                dup_button.config(state=tk.NORMAL)
            status_label.config(text="Indexing failed", fg="red")
            progress_label.config(text="")
            try:
                progress_bar.stop()
            except Exception:
                pass
            progress_bar._running = False
            progress_bar.config(mode="determinate")
            progress_bar['value'] = 0
            progress_bar.pack_forget()
            return
    except Empty:
        pass
    
    if index_thread and index_thread.is_alive():
        root.after(100, lambda: update_indexing_then_search(queue, keyword))
    else:
        search_button.config(state=tk.NORMAL)
        if 'dup_button' in globals():
            dup_button.config(state=tk.NORMAL)

def start_duplicate_scan():
    global search_results, duplicate_thread, stop_event
    base_dir = directory_path.get().strip()
    if not base_dir or not os.path.isdir(base_dir):
        messagebox.showerror("Invalid Path", "Please select a valid directory first.")
        return

    # Clear previous results
    for item in results_tree.get_children():
        results_tree.delete(item)
    search_results = []

    # UI state
    search_button.config(state=tk.DISABLED)
    dup_button.config(state=tk.DISABLED)
    cancel_button.config(state=tk.NORMAL)
    status_frame.pack(pady=10, fill=tk.X, padx=10)
    status_label.pack(fill=tk.X, padx=5)
    progress_label.pack(fill=tk.X, padx=5)
    status_label.config(text="Finding duplicates...", fg="blue")
    progress_label.config(text="Starting duplicate scan (min size 1 MB)...")

    # Queue and thread
    result_queue = Queue()
    stop_event = threading.Event()
    duplicate_thread = DuplicateThread(
        file_index,
        result_queue,
        stop_event,
        min_size_bytes=1 * 1024 * 1024,
        base_dir=os.path.normpath(base_dir),
    )
    duplicate_thread.start()

    update_duplicate_results(result_queue)

def update_results(queue):
    finished = False
    max_items = 50
    for _ in range(max_items):
        try:
            msg_type, data = queue.get_nowait()
        except Empty:
            break

        if msg_type == 'result':
            file_info = data
            search_results.append(file_info['path'])

            filepath = file_info['path']
            filename = os.path.basename(filepath)

            # Insert file directly as a flat list item
            results_tree.insert('', 'end', text=filename,
                                values=(filepath, filename,
                                        format_size(file_info['size']),
                                        format_date(file_info['modified'])),
                                tags=('file',))

            status_label.config(text=f"Found {len(search_results)} matches...", fg="green")
        elif msg_type == 'progress':
            current, total = data
            progress_label.config(text=f"Processing: {current} of {total} matches")
            print(f"Search progress: {current}/{total}")
        elif msg_type == 'done':
            # Search completed
            search_button.config(state=tk.NORMAL)
            cancel_button.config(state=tk.DISABLED)
            progress_label.config(text="")
            if not search_results:
                status_label.config(text="Search complete - No matches found", fg="orange")
            else:
                status_label.config(text=f"Search complete - Found {len(search_results)} matches", fg="green")
            finished = True
            break
        elif msg_type == 'error':
            messagebox.showerror("Error", f"An error occurred during search: {data}")
            search_button.config(state=tk.NORMAL)
            cancel_button.config(state=tk.DISABLED)
            status_frame.pack_forget()
            finished = True
            break

    if not finished and search_thread and search_thread.is_alive():
        root.after(100, lambda: update_results(queue))

def update_duplicate_results(queue):
    finished = False
    max_items = 20
    for _ in range(max_items):
        try:
            msg_type, data = queue.get_nowait()
        except Empty:
            break

        if msg_type == 'dup_progress':
            stage, current, total = data
            progress_label.config(text=f"{stage}: {current:,} of {total:,}")
        elif msg_type == 'dup_group':
            group = data
            # Root item per duplicate group
            group_text = f"{group['count']} duplicates - {format_size(group['size'])} each"
            parent_id = results_tree.insert('', 'end', text=group_text,
                                            values=(group['hash'], '', '', ''),
                                            tags=('directory',))
            # Add files as children
            for info in sorted(group['files'], key=lambda x: x['path']):
                filepath = info['path']
                filename = os.path.basename(filepath)
                results_tree.insert(parent_id, 'end', text=filename,
                                    values=(filepath, filename,
                                            format_size(info.get('size', 0)),
                                            format_date(info.get('modified', 0))),
                                    tags=('file',))
            status_label.config(text="Duplicate groups updating...", fg="green")
        elif msg_type == 'dup_done':
            search_button.config(state=tk.NORMAL)
            dup_button.config(state=tk.NORMAL)
            cancel_button.config(state=tk.DISABLED)
            progress_label.config(text="")
            if not results_tree.get_children(''):
                status_label.config(text="No duplicate groups found", fg="orange")
            else:
                status_label.config(text="Duplicate scan complete", fg="green")
            finished = True
            break
        elif msg_type == 'error':
            messagebox.showerror("Error", f"An error occurred: {data}")
            search_button.config(state=tk.NORMAL)
            dup_button.config(state=tk.NORMAL)
            cancel_button.config(state=tk.DISABLED)
            status_frame.pack_forget()
            finished = True
            break

    if not finished and duplicate_thread and duplicate_thread.is_alive():
        root.after(100, lambda: update_duplicate_results(queue))

def open_file(event):
    selection = results_tree.selection()
    if not selection:
        return
    
    item = selection[0]
    item_data = results_tree.item(item)
    
    # Only open if it's a file (not a directory)
    if 'file' not in results_tree.item(item)['tags']:
        return
    
    filepath = item_data['values'][0]
    if not os.path.exists(filepath):
        messagebox.showerror("Error", "File not found")
        return
    
    try:
        if platform.system() == "Windows":
            if filepath.lower().endswith('.pdf'):
                try:
                    os.startfile(os.path.normpath(filepath))
                except Exception:
                    try:
                        subprocess.Popen(['start', '', filepath], shell=False)
                    except Exception:
                        subprocess.Popen(['start', '', filepath], shell=True)
            else:
                os.startfile(filepath)
        elif platform.system() == "Darwin":  # macOS
            subprocess.call(("open", filepath))
        else:  # Linux
            subprocess.call(("xdg-open", filepath))
    except Exception as e:
        error_msg = str(e)
        messagebox.showerror("Error", f"Could not open file: {error_msg}\nPath: {filepath}")
        print(f"Error opening file: {error_msg}")
        print(f"File path: {filepath}")

def delete_file(event=None):
    selection = results_tree.selection()
    if not selection:
        return
    
    item = selection[0]
    item_data = results_tree.item(item)
    
    # Only delete if it's a file (not a directory)
    if 'file' not in results_tree.item(item)['tags']:
        return
    
    filepath = item_data['values'][0]
    filename = item_data['values'][1]
    
    if not os.path.exists(filepath):
        messagebox.showerror("Error", "File not found")
        return
    
    if not messagebox.askyesno("Confirm Delete", 
                              f"Are you sure you want to delete:\n{filename}\n\nFrom location:\n{filepath}",
                              icon='warning'):
        return
    
    try:
        os.remove(filepath)
        # Remove from tree and results
        results_tree.delete(item)
        if filepath in search_results:
            search_results.remove(filepath)
        
        # If parent has no more children, remove it too
        parent = results_tree.parent(item)
        if parent and not results_tree.get_children(parent):
            results_tree.delete(parent)
        
        messagebox.showinfo("Success", f"File deleted successfully:\n{filename}")
    except Exception as e:
        messagebox.showerror("Error", f"Could not delete file: {str(e)}")

def show_context_menu(event):
    item = results_tree.identify_row(event.y)
    if item:
        results_tree.selection_set(item)
        # Only show context menu for files, not directories
        if 'file' in results_tree.item(item)['tags']:
            context_menu.post(event.x_root, event.y_root)

def cancel_search():
    cancelled_any = False
    if 'search_thread' in globals() and search_thread and search_thread.is_alive():
        stop_event.set()
        search_button.config(state=tk.NORMAL)
        cancelled_any = True
    if 'duplicate_thread' in globals() and duplicate_thread and duplicate_thread.is_alive():
        stop_event.set()
        dup_button.config(state=tk.NORMAL)
        cancelled_any = True
    cancel_button.config(state=tk.DISABLED)
    if cancelled_any:
        status_label.config(text="Operation cancelled", fg="red")
    status_frame.pack_forget()

# GUI setup
root = tk.Tk()
root.title("Company Server File Search")
root.geometry("700x500")

directory_path = tk.StringVar()

# Main content frame
main_frame = tk.Frame(root)
main_frame.pack(expand=True, fill=tk.BOTH, padx=10, pady=5)

# Search inputs
tk.Label(main_frame, text="Enter keyword to search for:").pack(pady=5)
keyword_entry = tk.Entry(main_frame, width=50)
keyword_entry.pack(pady=5)

tk.Label(main_frame, text="Select server directory to search in:").pack(pady=5)
tk.Entry(main_frame, textvariable=directory_path, width=50).pack(pady=5)
tk.Button(main_frame, text="Browse", command=browse_directory).pack(pady=5)

# Search and Cancel buttons
button_frame = tk.Frame(main_frame)
button_frame.pack(pady=5)
search_button = tk.Button(button_frame, text="Search", command=start_search, bg="lightblue", width=15)
search_button.pack(side=tk.LEFT, padx=5)
dup_button = tk.Button(button_frame, text="Find Duplicates", command=start_duplicate_scan, width=15)
dup_button.pack(side=tk.LEFT, padx=5)
cancel_button = tk.Button(button_frame, text="Cancel", command=cancel_search, state=tk.DISABLED, width=15)
cancel_button.pack(side=tk.LEFT, padx=5)

# Status frame with progress indicators
status_frame = tk.Frame(main_frame, relief=tk.GROOVE, bd=1)
status_label = tk.Label(status_frame, text="", font=("Arial", 10, "bold"))
progress_label = tk.Label(status_frame, text="")
progress_bar = ttk.Progressbar(status_frame, orient="horizontal", mode="determinate", length=100, maximum=100)

# Results area
results_frame = tk.Frame(main_frame)
results_frame.pack(expand=True, fill=tk.BOTH, pady=5)

# Create Treeview
results_tree = ttk.Treeview(results_frame, columns=('path', 'name', 'size', 'modified'),
                           displaycolumns=('path', 'name', 'size', 'modified'))
results_tree.heading('path', text='Location')
results_tree.heading('name', text='Name')
results_tree.heading('size', text='Size')
results_tree.heading('modified', text='Modified')

# Configure column widths
results_tree.column('path', width=350)
results_tree.column('name', width=200)
results_tree.column('size', width=100)
results_tree.column('modified', width=150)

# Configure tags for different row types
results_tree.tag_configure('directory', foreground='navy')
results_tree.tag_configure('file', foreground='black')

# Add scrollbars
vsb = ttk.Scrollbar(results_frame, orient="vertical", command=results_tree.yview)
hsb = ttk.Scrollbar(results_frame, orient="horizontal", command=results_tree.xview)
results_tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

# Grid layout for tree and scrollbars
results_tree.grid(column=0, row=0, sticky='nsew')
vsb.grid(column=1, row=0, sticky='ns')
hsb.grid(column=0, row=1, sticky='ew')
results_frame.grid_columnconfigure(0, weight=1)
results_frame.grid_rowconfigure(0, weight=1)

# Bind events
results_tree.bind("<Double-1>", open_file)
results_tree.bind("<Button-3>", show_context_menu)

# Create context menu
context_menu = tk.Menu(root, tearoff=0)
context_menu.add_command(label="Open", command=lambda: open_file(None))
context_menu.add_separator()
context_menu.add_command(label="Delete", command=lambda: delete_file(None))

file_index = FileIndex()
print("File index initialized")

root.mainloop()
