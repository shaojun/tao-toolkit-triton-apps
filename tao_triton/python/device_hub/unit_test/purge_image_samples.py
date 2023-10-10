import os
import datetime

# specify the root directory to search in
root_dir = '/media/kevin/DATA1/tao-toolkit-triton-apps/tao_triton/python/device_hub/ebic_image_samples'

if __name__ == '__main__':
    # calculate the cutoff date (20 days ago)
    cutoff_date = datetime.datetime.now() - datetime.timedelta(days=45)

    # loop through all subdirectories in the root directory
    for dirpath, dirnames, filenames in os.walk(root_dir):
        total_count = len(filenames)
        # initialize a counter for the number of removed files in this subdirectory
        removed_count = 0
        
        # loop through all files in the current subdirectory
        for filename in filenames:
            # construct the full path to the file
            file_path = os.path.join(dirpath, filename)
            
            # get the creation time of the file
            creation_time = datetime.datetime.fromtimestamp(os.path.getctime(file_path))
            
            # check if the file was created more than 20 days ago
            if creation_time < cutoff_date:
                # delete the file
                os.remove(file_path)
                # increment the removed count
                removed_count += 1
        
        # print the total number of files and removed files for this subdirectory
        print(f"Removed {removed_count} out of {total_count} files from {dirpath}")