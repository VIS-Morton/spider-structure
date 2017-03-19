# -*- coding: utf8 -*-
import sys
import logging
from functools import partial

import gevent
import requests
from lxml import etree
from gevent import queue, pool, monkey

reload(sys)
sys.setdefaultencoding("utf8")

logger = logging.getLogger("sohu")
logger.setLevel(logging.INFO)
formatter = logging.Formatter("[%(asctime)s %(levelname)s] %(message)s")

file_handler = logging.FileHandler("sohu")
error_file_handler = logging.FileHandler("sohu-error")
error_file_handler.setLevel(logging.ERROR)

stream_handler = logging.StreamHandler()

file_handler.setFormatter(formatter)
error_file_handler.setFormatter(formatter)
stream_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(error_file_handler)
logger.addHandler(stream_handler)


BASE_URL = "https://m.sohu.com/"
DEDUPLICATE_QUEUE = queue.Queue()
DEDUPLICATE_QUEUE.put({BASE_URL, })


monkey.patch_all()


def get_urls(cur_url, total_urls, valid_urls,
             invalid_urls, error_urls, finished_urls):
    try:
        new_urls = set()
        resp = requests.get(cur_url, stream=True, timeout=30)
    except requests.Timeout:
        error_urls.add(cur_url)
        logger.error("failed with read timeout url: {} ".format(cur_url))
    except Exception, e:
        error_urls.add(cur_url)
        logger.error("failed with error: {} url: {} ".format(
            e.message, cur_url))
    else:
        status_code = resp.status_code
        if status_code / 100 in (4, 5):
            invalid_urls.add(cur_url)
            logger.error("status_code: {} url: {}".format(
                status_code, cur_url))
        else:
            valid_urls.add(cur_url)
            content = resp.content
            html = etree.HTML(content)
            urls = set([i.strip() for i in html.xpath("//a/@href")])  # 去重
            filtered_urls = []
            # 清洗URL
            for url in urls:
                if url.startswith("#"):
                    continue
                elif url.startswith("javascript"):
                    continue
                elif url.startswith("http"):  # 包括http&https
                    if "m.sohu.com" in url:
                        filtered_urls.append(url)
                    else:
                        continue  # 只记录m.sohu.com域名下链接
                # 相对路径
                elif url.startswith("/"):
                    filtered_urls.append("https://m.sohu.com" + url)
                else:
                    filtered_urls.append("https://m.sohu.com/" + url)
            new_urls = set(filtered_urls) - total_urls  # 获取新的url
            total_urls |= new_urls  # 更新所有的url
    finally:
        finished_urls.add(cur_url)
        if new_urls:
            logger.info("add new_urls: {}".format(len(new_urls)))
            DEDUPLICATE_QUEUE.put(set(new_urls))  # 把新url送队列， switch


def get_speed(interval=60, **kw):
    while True:
        last_count = len(kw["finished_urls"])
        gevent.sleep(interval)
        count = len(kw["finished_urls"])
        logger.info("%s urls finished in last minute" % (count - last_count))
        logger.info(",".join(
            ["{}: {}".format(k, len(v)) for k, v in kw.iteritems()]))


def deduplicate_urls(concurrency=10, **kw):

    """
    程序只迭代m.sohu.com域名下链接，
    没有针对不同子板下的链接html格式做单独处理，采用比较通用的方式递归，没有对页面进行性能争对优化
    公共gevent的pool做并发处理，gevent优于thread做这样的io密集操作
    分别记录所有的url，可连通的url，4xx或5xx的url以及报错的url到txt
    通过队列传递url， 可以认为既是消费者也是生产者
    没有通过代理ip切换去抓取数据，可能会被会被封掉爬虫
    """
    work_pool = pool.Pool(size=concurrency)
    func = partial(get_urls, **kw)
    try:
        while True:
            new_urls = DEDUPLICATE_QUEUE.get(timeout=60)
            for i in new_urls:
                work_pool.add(gevent.spawn(func, i))
                gevent.sleep(0)
    except gevent.Timeout:
        work_pool.join()  # 完成剩余未完成对请求
        logger.info("FINISHED!")
    except Exception, e:
        work_pool.kill()
        logging.error(e, exc_info=1)


if __name__ == "__main__":
    total_urls = set()  # 所有的url
    valid_urls = set()  # 合法的url
    invalid_urls = set()  # 4XX或者5XX的url
    error_urls = set()  # 请求报错的url
    finished_urls = set()  # 当前已经扫描的url
    kwargs = {
        "total_urls": total_urls,
        "valid_urls": valid_urls,
        "invalid_urls": invalid_urls,
        "error_urls": error_urls,
        "finished_urls": finished_urls
    }
    try:
        gevent.spawn(get_speed, **kwargs)
        gevent.spawn(deduplicate_urls, **kwargs).join()
    except KeyboardInterrupt:
        logger.info("EXIT!")
    finally:
        with open("invalid_urls.txt", "w") as f:
            for i in invalid_urls:
                f.write(i + "\n")
        with open("error_urls.txt", "w") as f:
            for i in error_urls:
                f.write(i + "\n")
        with open("valid_urls.txt", "w") as f:
            for i in valid_urls:
                f.write(i + "\n")
        with open("total_urls.txt", "w") as f:
            for i in total_urls:
                f.write(i + "\n")
