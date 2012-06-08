import sys
import os
import os.path

def get_size(filepath):
  assert(os.path.exists(filepath)), "The path {0} does not exist!:".format(filepath)  # Test to make sure quad_path exists
  if os.path.isdir(filepath) is True:
    file_size = 0
    for dirpath, dirnames, filenames in os.walk(filepath):
      for file in filenames:  file_size += os.path.getsize(os.path.join(dirpath, file))  # pass
  else:
    file_size = os.path.getsize(raw_quad_path)
  filesize_mb = float(file_size)/float(1000000)
  filesize_gb = float(filesize_mb)/float(1024)
  return {'bytes': file_size, 'MB': filesize_mb, 'GB': filesize_gb, }