# coding: utf-8

from threading import current_thread
import multiprocessing, time, random
import requests
import json
import shutil

from path import Path
from rx import Observable
from rx.concurrency import ThreadPoolScheduler



def need_download(tup):
    if tup is None or len(tup) != 2:
        return False
    print(tup)
    print("**********************")
    filepath = download_dir / tup[0]
    exists = filepath.exists()
    if not exists:
        return  True
    ##print("file {} exists {}".format(filepath, exists))
    md5 = filepath.read_hexhash('md5')
    print("{} md5 {}".format(filepath, md5))
    bad_crc = md5 != tup[1]['md5']
    print("Bad crc {}".format(bad_crc))
    return bad_crc

def download(url, download_dir):
    try:
        path = download_dir + url.split("/")[-1] 
        path_tmp = path + ".conda-tmp"
        #print("Downalod " + url)
        r = requests.get(url, stream=True)
        if r.status_code == 200:
            with open(path_tmp, 'wb') as f:
                r.raw.decode_content = True
                shutil.copyfileobj(r.raw, f)
        shutil.move(path_tmp, path)
        #print("Save " + path)
        return path
    except Exception as ex:
        print(ex.msg)


repo_url = "https://conda.anaconda.org/asmeurer/linux-64/"
download_dir = Path("d:\\tmp2\\")
repo_data_file = download(repo_url + "repodata.json", download_dir)


#todo: remove pending .conda-tmp files

with open(repo_data_file) as data_file:    
    repo_data = json.load(data_file)


optimal_thread_count = multiprocessing.cpu_count() + 1
pool_scheduler = ThreadPoolScheduler(optimal_thread_count)

    #.take(100) \

Observable.from_(repo_data['packages'].items()) \
        .flat_map(lambda s: Observable.just(s) \
        .subscribe_on(pool_scheduler) 
        .filter(need_download) \
        .map(lambda s: download(repo_url + s[0], download_dir)) \
    ) \
    .subscribe(on_next=lambda i: print("File {} saved in thread {}".format(i, current_thread().name)),
               on_error=lambda e: print(e),
               on_completed=lambda: print("PROCESS 1 done!"))

input("Press any key to exit\n")

