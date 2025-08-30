import requests
from bs4 import BeautifulSoup
import time
import os
import re
from urllib.parse import urljoin, urlparse, unquote

# --- 配置区 ---
GROUP_ID = ""
GROUP_BASE_URL = f"https://www.douban.com/group/{GROUP_ID}"

# !!! 你的Cookie，注意它有有效期 !!!
COOKIES = ''

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36',
    'Cookie': COOKIES,
    'Referer': f'https://www.douban.com/group/{GROUP_ID}/discussion'
}

DATASET_BASE_DIR = f"douban_dataset"
if not os.path.exists(DATASET_BASE_DIR):
    os.makedirs(DATASET_BASE_DIR)

REQUEST_DELAY = 2
MAX_PAGES_TOPICS = 100
MAX_ORIGINAL_IMAGES_PER_POST = 10
MAX_EDITED_IMAGES_PER_POST = 20


# --- 辅助函数 (无变化) ---
def get_file_extension(url):
    try:
        path = urlparse(url).path
        ext = os.path.splitext(path)[1]
        if ext and ext.lower() in ['.jpg', '.jpeg', '.png', '.gif', '.webp']:
            return ext
    except:
        pass
    return '.jpg'


def sanitize_filename(filename):
    return re.sub(r'[<>:"/\\|?*]', '_', filename)


def get_html_content(url, retries=3):
    for i in range(retries):
        try:
            response = requests.get(url, headers=HEADERS, timeout=30)
            response.raise_for_status()
            response.encoding = response.apparent_encoding
            return response.text
        except requests.exceptions.Timeout:
            print(f"      请求超时: {url} (尝试 {i + 1}/{retries})")
            time.sleep(REQUEST_DELAY * (i + 1.5))
        except requests.exceptions.RequestException as e:
            status_code = e.response.status_code if e.response is not None else "N/A"
            print(f"      请求失败: {url} - {e} (状态码: {status_code}, 尝试 {i + 1}/{retries})")
            if status_code == 403 or status_code == 404:
                print(f"        状态码 {status_code}，可能是Cookie失效/权限问题或帖子不存在。")
                return None
            time.sleep(REQUEST_DELAY * (i + 1.5))
        except Exception as e:
            print(f"      发生未知错误: {url} - {e} (尝试 {i + 1}/{retries})")
            time.sleep(REQUEST_DELAY * (i + 1.5))
    print(f"    获取内容失败，已达最大重试次数: {url}")
    return None


# --- 核心功能函数 ---
def get_topic_links_from_group_page(group_page_url):
    html_content = get_html_content(group_page_url)
    if not html_content: return []
    soup = BeautifulSoup(html_content, 'html.parser')
    topic_links = []
    for link_tag in soup.select('table.olt td.title a[href*="/group/topic/"]'):
        href = link_tag.get('href')
        # 移除URL中的查询参数，确保链接纯净
        parsed_href = urlparse(href)
        clean_href = parsed_href._replace(query='').geturl()
        if clean_href and clean_href not in topic_links:
            topic_links.append(clean_href)
    return topic_links

def pic_id(url: str) -> str | None:
    path = urlparse(url).path          # /view/group_topic/xl/public/p689889440.jpg
    m = re.search(r'p(\d+)\.(?:jpg|webp)', path, re.I)
    return m.group(1) if m else None

QUALITY_ORDER = {'xl': 3, 'l': 2, 'm': 1}   # 级别越高越清晰

def quality_score(url: str) -> int:
    m = re.search(r'/(xl|l|m)/', url, re.I)    # x / l / m
    return QUALITY_ORDER.get(m.group(1).lower(), 0) if m else 0

def dedup_urls(urls: list[str]) -> list[str]:
    id2best = {}
    for u in urls:
        pid = pic_id(u)
        if not pid:               # 没取到 pXXXX —— 忽略或单独处理
            continue
        # 如果这个 ID 第一次出现，或找到更高清的版本，就替换
        if (pid not in id2best) or quality_score(u) > quality_score(id2best[pid]):
            id2best[pid] = u
    # 保持原顺序：按 urls 顺序收集去重后的 URL
    seen = set()
    result = []
    for u in urls:
        pid = pic_id(u)
        pick = id2best.get(pid)
        if pick and pick not in seen:
            result.append(pick)
            seen.add(pick)
    return result

def process_topic_page(topic_url, topic_id_str, post_index, total_posts):
    print(f"  [处理开始] 帖子 {post_index}/{total_posts} (ID: {topic_id_str}) URL: {topic_url}")

    html_content = get_html_content(topic_url)
    if not html_content:
        print(f"  [处理失败] 无法获取帖子内容: {topic_url}\n")
        return False

    soup = BeautifulSoup(html_content, 'html.parser')

    topic_dir = os.path.join(DATASET_BASE_DIR, topic_id_str)
    edited_images_dir = os.path.join(topic_dir, "edited_images")
    if not os.path.exists(topic_dir): os.makedirs(topic_dir)
    if not os.path.exists(edited_images_dir): os.makedirs(edited_images_dir)

    op_title = "Untitled"
    title_element = soup.select_one('div.article h1')  # 主标题选择器
    if not title_element:  # 尝试备用标题选择器
        title_element = soup.select_one('div.title h1')  # 比如在日记页面可能用这个
    if title_element: op_title = title_element.get_text(strip=True)

    op_main_text_content = ""
    # 增强 op_content_element 的选择器
    ### EDITED op_content_element = soup.select_one('div.topic-richtext, div.topic-content')
    op_content_element = soup.select_one('#link-report .topic-content, #link-report .topic-richtext, div.article .topic-content, div.article .topic-doc, .topic-content.clearfix, .topic-richtext') #div.rich-content.topic-richtext, div.topic-richtext, div.topic-content, div.richtext, div.topic-content clearfix')
    if not op_content_element:  # 如果常用选择器找不到，尝试备用选择器
        op_content_element = soup.select_one('div.article div.richtext, div.article div.note')  # 豆瓣日记等其他类型帖子可能用这些
        if op_content_element:
            print(f"    [信息] 帖子 {topic_id_str}: op_content_element 使用备用选择器找到。")

    if op_content_element:
        op_main_text_content = op_content_element.get_text(separator='\n', strip=True)
        if not op_main_text_content.strip():
            text_parts = []
            for element in op_content_element.children:
                if element.name == 'p':
                    text_parts.append(element.get_text(strip=True))
                elif element.name == 'br':
                    if text_parts and text_parts[-1] != '\n': text_parts.append('\n')
                elif not element.name and isinstance(element, str):
                    stripped_text = element.strip()
                    if stripped_text: text_parts.append(stripped_text)
            op_main_text_content = "\n".join(filter(None, text_parts))
    else:
        op_main_text_content = "Error: Could not find OP main text content element."
        print(f"    [警告] 帖子 {topic_id_str}: 未找到 op_content_element (楼主主要文字区域)。")

    full_prompt_text = f"Title: {op_title}\n\n{op_main_text_content}"
    try:
        with open(os.path.join(topic_dir, "prompt.txt"), "w", encoding="utf-8") as f:
            f.write(full_prompt_text)
    except Exception as e:
        print(f"    [错误] 帖子 {topic_id_str}: 保存prompt.txt失败 - {e}")

    original_image_urls = []

    #print("HTML size:", len(html_content))
    if not soup.select_one("div.horizontal-photos"):
        m = re.search(r'window\._CONFIG\.topic\[\'photos\']\s*=\s*(\[[^\]]+\])', html_content)
        if m:
            import json
            #print(m.group(1))
            for ph in json.loads(m.group(1)):
                raw = ph["image"]["raw"]["url"]
                large = ph["image"]["large"]["url"]
                original_image_urls.append(raw or large)

    if op_content_element:
        for img_tag in op_content_element.select('div.img-container img, div.image-wrapper img, div.photo-item img, div.photo-img img, div.horizontal-photos img, img'): ### EDITED op_content_element.select('img'):  # 获取所有图片标签
            src = img_tag.get('src')
            data_src = img_tag.get('data-src')  # 常见懒加载属性
            data_original = img_tag.get('data-original')  # 另一个常见懒加载属性

            actual_src = None
            if data_src:  # 优先使用 data-src
                actual_src = data_src
            elif data_original:  # 其次使用 data-original
                actual_src = data_original
            elif src and not src.startswith('data:image'):  # 最后使用 src，但排除 base64 编码的图片
                actual_src = src

            # 如果 src 是一个已知的占位符，并且有 data_src 或 data_original，则优先它们
            if src and ('占位图' in src or 'blank.gif' in src or src.startswith('data:image')) and (
                    data_src or data_original):
                if data_src:
                    actual_src = data_src
                elif data_original:
                    actual_src = data_original

            if actual_src and actual_src.startswith('//'):
                actual_src = 'https:' + actual_src

            if actual_src and 'doubanio.com' in actual_src:
                if '/icon/' not in actual_src and 'avatar' not in actual_src.lower() and \
                        '/small/' not in actual_src and '/bn/' not in actual_src and \
                        'grey.gif' not in actual_src:  # 进一步排除豆瓣的灰色占位符
                    if actual_src not in original_image_urls:
                        original_image_urls.append(actual_src)

    #original_image_urls = list(dict.fromkeys(original_image_urls))[:MAX_ORIGINAL_IMAGES_PER_POST]
    original_image_urls = dedup_urls(original_image_urls)
    original_image_urls = list(dict.fromkeys(original_image_urls))[:MAX_ORIGINAL_IMAGES_PER_POST]

    op_image_download_count = 0
    for i, img_url in enumerate(original_image_urls):
        if download_image(img_url, topic_dir, f"original_{i}"):
            op_image_download_count += 1

    comments_section = soup.select(
        'ul#comments > li.comment-item, div.comment-list > div.comment-item, li.clearfix.comment-item')
    edited_image_count = 0

    if comments_section:
        for idx, comment_item in enumerate(comments_section):
            if edited_image_count >= MAX_EDITED_IMAGES_PER_POST:
                break

            comment_content_area = comment_item.select_one('div.reply-doc > div.bd, div.comment-content, div.richtext, div.cmt-img-wrapper, div.cmt-img') ###EDITED comment_item.select_one('div.reply-doc > div.bd, div.comment-content, div.richtext')
            if not comment_content_area: comment_content_area = comment_item

            if comment_content_area:
                current_comment_found_urls = []
                for img_tag in comment_content_area.select('img'):
                    if edited_image_count >= MAX_EDITED_IMAGES_PER_POST:
                        break

                    src = img_tag.get('src')
                    data_src = img_tag.get('data-src')
                    data_original = img_tag.get('data-original')
                    actual_src = None

                    if data_src:
                        actual_src = data_src
                    elif data_original:
                        actual_src = data_original
                    elif src and not src.startswith('data:image'):
                        actual_src = src

                    if src and ('占位图' in src or 'blank.gif' in src or src.startswith('data:image')) and (
                            data_src or data_original):
                        if data_src:
                            actual_src = data_src
                        elif data_original:
                            actual_src = data_original

                    if actual_src and actual_src.startswith('//'):
                        actual_src = 'https:' + actual_src

                    if actual_src and 'doubanio.com' in actual_src:
                        if '/icon/' not in actual_src and 'avatar' not in actual_src.lower() and \
                                '/small/' not in actual_src and '/bn/' not in actual_src and \
                                'grey.gif' not in actual_src:
                            if actual_src not in current_comment_found_urls and actual_src not in original_image_urls:
                                current_comment_found_urls.append(actual_src)
                                if download_image(actual_src, edited_images_dir,
                                                  f"edited_{topic_id_str}_{edited_image_count}"):
                                    edited_image_count += 1
                if edited_image_count >= MAX_EDITED_IMAGES_PER_POST:
                    break

    prompt_summary = op_main_text_content[:10].replace('\n', ' ').replace('\r', ' ') + (
        '...' if len(op_main_text_content) > 10 else '')
    print(
        f"    [处理结果] Prompt: '{op_title[:20].replace('\n', ' ').replace('\r', ' ')}...' / '{prompt_summary}'. 原图: {op_image_download_count} 张. 编辑图: {edited_image_count}/{MAX_EDITED_IMAGES_PER_POST} 张.")
    print(f"  [处理完毕] 帖子 ID: {topic_id_str} 数据保存完毕.\n")
    return True


def download_image(image_url, save_dir, base_filename):
    if not image_url: return False
    try:
        ext = get_file_extension(image_url)
        file_name = sanitize_filename(f"{base_filename}{ext}")
        save_path = os.path.join(save_dir, file_name)
        if os.path.exists(save_path):
            return True
        img_response = requests.get(image_url, headers=HEADERS, stream=True, timeout=45)
        img_response.raise_for_status()
        with open(save_path, 'wb') as f:
            for chunk in img_response.iter_content(chunk_size=81920): f.write(chunk)
        return True
    except requests.exceptions.RequestException as e:
        print(f"      下载图片失败: {image_url[:100]}... - {e}")
    except Exception as e:
        print(f"      保存图片时发生错误: {image_url[:100]}... - {e}")
    return False


# --- 主逻辑 ---
def main():
    if not COOKIES or COOKIES == '你的Cookie字符串放在这里':
        if COOKIES == '你的Cookie字符串放在这里':  # 确保用户真的替换了Cookie
            print("错误：请在脚本中配置你的豆瓣有效Cookie！否则很可能无法爬取或被限制。")
            user_input = input("是否在没有Cookie的情况下继续尝试 (y/N)? ").lower()
            if user_input != 'y': return

    print(f"开始爬取豆瓣小组: {GROUP_BASE_URL}")
    print(f"数据将保存到: {DATASET_BASE_DIR}")
    print(
        f"请求间隔: {REQUEST_DELAY}秒, 每个帖子最多下载 {MAX_ORIGINAL_IMAGES_PER_POST} 张原图, 评论区最多 {MAX_EDITED_IMAGES_PER_POST} 张编辑图。")
    print(f"!!! 测试模式: MAX_PAGES_TOPICS = {MAX_PAGES_TOPICS} !!!")

    all_topic_links_collected = []
    base_list_url = f"{GROUP_BASE_URL.rstrip('/')}/discussion"

    ### Start from page
    START_PAGE = 0

    for page_num in range(START_PAGE, MAX_PAGES_TOPICS):
        start_index = page_num * 25
        current_page_url = f"{base_list_url}?start={start_index}"
        print(f"获取帖子列表第 {page_num + 1} 页: {current_page_url}")
        new_links = get_topic_links_from_group_page(current_page_url)
        if not new_links:
            print(f"第 {page_num + 1} 页没有获取到新的帖子链接。")
            break
        added_count = 0
        for link in new_links:
            if link not in all_topic_links_collected:
                all_topic_links_collected.append(link)
                added_count += 1
        print(f"本页获取到 {added_count} 个新帖子链接。当前总计 {len(all_topic_links_collected)} 个独立帖子链接。")
        if page_num < MAX_PAGES_TOPICS - 1: time.sleep(REQUEST_DELAY)

    print(f"\n总共收集到 {len(all_topic_links_collected)} 个不同的帖子链接。\n")
    processed_topics_count = 0;
    failed_topics_count = 0
    total_links_to_process = len(all_topic_links_collected)

    for i, topic_url in enumerate(all_topic_links_collected):
        topic_id_match = re.search(r'/topic/(\d+)/?', topic_url)
        if not topic_id_match:
            print(f"    无法从URL {topic_url} 中提取 topic_id，跳过此帖。\n")
            failed_topics_count += 1
            continue
        current_topic_id_str = topic_id_match.group(1)

        if process_topic_page(topic_url, current_topic_id_str, i + 1, total_links_to_process):
            processed_topics_count += 1
        else:
            failed_topics_count += 1

        if i < total_links_to_process - 1: time.sleep(REQUEST_DELAY)

    print(f"\n所有任务完成！成功处理 {processed_topics_count} 个帖子，失败或跳过 {failed_topics_count} 个帖子。")


if __name__ == "__main__":
    main()