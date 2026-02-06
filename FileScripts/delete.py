import os


def delete(root_path, 
            extension=None, contains=None, 
            delete_all=False, rootFolderOnly=False, 
            ignore_extension=None, ignore_contains=None):
    try:
        for root, dirs, files in os.walk(root_path):
            if rootFolderOnly:
                dirs.clear()  # Clear dirs to prevent descending into subdirectories

            for file in files:
                if (ignore_extension and file.endswith(ignore_extension)) or (ignore_contains and ignore_contains in file):
                    continue
                
                if delete_all:
                    file_path = os.path.join(root, file)
                    os.remove(file_path)
                    print(f"Deleted: {file_path}")
                    continue
                
                if file.endswith(extension) or (contains and contains in file):
                    file_path = os.path.join(root, file)
                    os.remove(file_path)
                    print(f"Deleted: {file_path}")
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    delete("root/folderPath", extension=".txt", contains='test')
    # or
    delete("root\\folderPath", delete_all=True)  # delete all files in the specified folder and its subfolders
    # or
    delete("root\\folderPath", delete_all=True, rootFolderOnly=True)  # delete all files in the specified folder only (no subfolders)
