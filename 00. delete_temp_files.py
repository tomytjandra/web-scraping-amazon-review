import glob
import shutil
from tqdm import tqdm

username = "tomyt"

try:
    for f in tqdm(glob.glob(f"C:/Users/{username}/AppData/Local/Temp/scoped_dir*")):
        shutil.rmtree(f)
except Exception as e:
        print(e)