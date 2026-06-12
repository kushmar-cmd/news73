import os
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from flask import Flask, jsonify, render_template
import requests
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

app = Flask(__name__)

FEEDS = {
    "ישראל": [
        ("Ynet חדשות", "https://www.ynet.co.il/Integration/StoryRss2.xml", False),
        ("Walla ישראל", "https://rss.walla.co.il/feed/1", False),
        ("כאן חדשות", "https://www.kan.org.il/rss/", False),
        ("מאקו N12", "https://www.mako.co.il/rss/news.xml", False),
        ("הארץ", "https://www.haaretz.co.il/cmlink/1.1660017", False),
        ("ישראל היום", "https://www.israelhayom.co.il/rss.xml", False),
    ],
    "פוליטיקה ישראלית": [
        ("Ynet חדשות", "https://www.ynet.co.il/Integration/StoryRss2.xml", False),
        ("ישראל היום", "https://www.israelhayom.co.il/rss.xml", False),
        ("הארץ", "https://www.haaretz.co.il/cmlink/1.1660017", False),
        ("BBC Politics", "http://feeds.bbci.co.uk/news/politics/rss.xml", True),
        ("Politico", "https://www.politico.com/rss/politics08.xml", True),
    ],
    "עולם": [
        ("Walla עולם", "https://rss.walla.co.il/feed/2", False),
        ("BBC World", "http://feeds.bbci.co.uk/news/world/rss.xml", True),
        ("Guardian World", "https://www.theguardian.com/world/rss", True),
        ("Reuters World", "https://feeds.reuters.com/reuters/worldNews", True),
    ],
    "כלכלה": [
        ("Walla כלכלה", "https://rss.walla.co.il/feed/3", False),
        ("גלובס כלכלה", "https://www.globes.co.il/rss/rss.aspx?f=502", False),
        ("גלובס שוק ההון", "https://www.globes.co.il/rss/rss.aspx?f=561", False),
        ("TheMarker", "https://www.themarker.com/rss/", False),
        ("BBC Business", "http://feeds.bbci.co.uk/news/business/rss.xml", True),
        ("Reuters Business", "https://feeds.reuters.com/reuters/businessNews", True),
    ],
    "טכנולוגיה": [
        ("Walla טכנולוגיה", "https://rss.walla.co.il/feed/4", False),
        ("TechCrunch", "https://techcrunch.com/feed/", True),
        ("The Verge", "https://www.theverge.com/rss/index.xml", True),
        ("Wired", "https://www.wired.com/feed/rss", True),
    ],
    "ספורט": [
        ("Walla ספורט", "https://rss.walla.co.il/feed/7", False),
        ("ספורט 5", "https://www.sport5.co.il/rss.aspx", False),
        ("BBC Sport", "http://feeds.bbci.co.uk/sport/rss.xml", True),
        ("ESPN", "https://www.espn.com/espn/rss/news", True),
    ],
    "בידור ותרבות": [
        ("BBC Entertainment", "http://feeds.bbci.co.uk/news/entertainment_and_arts/rss.xml", True),
        ("Rolling Stone", "https://www.rollingstone.com/feed/", True),
    ],
    "בריאות": [
        ("BBC Health", "http://feeds.bbci.co.uk/news/health/rss.xml", True),
        ("WHO", "https://www.who.int/rss-feeds/news-releases.xml", True),
        ("WebMD", "https://rss.webmd.com/rss/rss.aspx?RSSSource=RSS_PUBLIC", True),
    ],
    "מדע וטבע": [
        ("Science Daily", "https://www.sciencedaily.com/rss/all.xml", True),
        ("NASA", "https://www.nasa.gov/rss/dyn/breaking_news.rss", True),
        ("New Scientist", "https://www.newscientist.com/feed/home/", True),
    ],
    "סביבה ואקלים": [
        ("BBC Environment", "http://feeds.bbci.co.uk/news/science_and_environment/rss.xml", True),
        ("Guardian Environment", "https://www.theguardian.com/environment/rss", True),
        ("Yale Environment", "https://e360.yale.edu/feed", True),
    ],
    "חינוך": [
        ("BBC Education", "http://feeds.bbci.co.uk/news/education/rss.xml", True),
        ("Times Higher Education", "https://www.timeshighereducation.com/news/rss.xml", True),
        ("EdSurge", "https://www.edsurge.com/news.rss", True),
    ],
}

cache = {}
cache_lock = threading.Lock()
CACHE_TTL = 300  # 5 minutes


def translate_text(text):
    if not text:
        return text
    try:
        url = "https://translate.googleapis.com/translate_a/single"
        params = {"client": "gtx", "sl": "en", "tl": "he", "dt": "t", "q": text[:500]}
        resp = requests.get(url, params=params, timeout=5,
                            headers={"User-Agent": "Mozilla/5.0"})
        if resp.status_code == 200:
            data = resp.json()
            return "".join(part[0] for part in data[0] if part[0]) or text
    except Exception:
        pass
    return text


def translate_articles(articles):
    to_translate = [a for a in articles if a.get("translate")]
    if not to_translate:
        return
    # Translate titles in parallel
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = {ex.submit(translate_text, a["title"]): a for a in to_translate}
        for f, a in futures.items():
            try:
                result = f.result(timeout=8)
                a["title"] = result
                a["translate"] = False
            except Exception:
                a["translate"] = False


def _clean(text):
    text = re.sub(r"<[^>]+>", "", text)
    return text.strip()


def parse_rss(xml_text, source_name, do_translate=False):
    items = []
    try:
        root = ET.fromstring(xml_text)
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        for item in root.findall(".//item")[:6]:
            title = item.findtext("title", "").strip()
            link = item.findtext("link", "").strip()
            desc = _clean(item.findtext("description", "").strip())[:200]
            pub = item.findtext("pubDate", "").strip()
            if title and link:
                items.append({"title": title, "link": link, "desc": desc,
                               "pub": pub, "source": source_name, "translate": do_translate})
        if not items:
            for entry in root.findall(".//atom:entry", ns)[:6]:
                title = entry.findtext("atom:title", "", ns).strip()
                link_el = entry.find("atom:link", ns)
                link = link_el.get("href", "") if link_el is not None else ""
                summary = _clean(entry.findtext("atom:summary", "", ns).strip())[:200]
                pub = entry.findtext("atom:updated", "", ns).strip()
                if title and link:
                    items.append({"title": title, "link": link, "desc": summary,
                                   "pub": pub, "source": source_name, "translate": do_translate})
    except Exception:
        pass
    return items


def fetch_feed(source_name, url, do_translate):
    try:
        resp = requests.get(url, timeout=8,
                            headers={"User-Agent": "Mozilla/5.0 (compatible; NewsApp/1.0)"})
        if resp.status_code == 200:
            if resp.encoding and resp.encoding.lower() in ("iso-8859-1", "windows-1252"):
                resp.encoding = resp.apparent_encoding
            return parse_rss(resp.text, source_name, do_translate)
    except Exception:
        pass
    return []


def fetch_category(category, feeds):
    articles = []
    with ThreadPoolExecutor(max_workers=6) as ex:
        futures = {ex.submit(fetch_feed, name, url, tr): name for name, url, tr in feeds}
        for f in as_completed(futures, timeout=15):
            try:
                articles.extend(f.result())
            except Exception:
                pass
    return articles


def refresh_cache():
    while True:
        def _fetch_one(cat_feeds):
            cat, feeds = cat_feeds
            articles = fetch_category(cat, feeds)
            translate_articles(articles)  # translate BEFORE caching
            with cache_lock:
                cache[cat] = {"articles": articles,
                               "updated": datetime.now(timezone.utc).isoformat()}

        with ThreadPoolExecutor(max_workers=4) as ex:
            list(ex.map(_fetch_one, FEEDS.items()))
        time.sleep(CACHE_TTL)


@app.route("/")
def index():
    return render_template("index.html", categories=list(FEEDS.keys()))


@app.route("/api/news")
def all_news():
    with cache_lock:
        return jsonify(dict(cache))


@app.route("/api/news/<category>")
def category_news(category):
    with cache_lock:
        data = cache.get(category)
    if not data:
        articles = fetch_category(category, FEEDS.get(category, []))
        translate_articles(articles)
        data = {"articles": articles, "updated": datetime.now(timezone.utc).isoformat()}
        with cache_lock:
            cache[category] = data
    return jsonify(data)


if __name__ == "__main__":
    print("טוען וּמתרגם חדשות... אנא המתן")

    def _initial_load(cat_feeds):
        cat, feeds = cat_feeds
        articles = fetch_category(cat, feeds)
        translate_articles(articles)  # translate BEFORE storing
        with cache_lock:
            cache[cat] = {"articles": articles,
                           "updated": datetime.now(timezone.utc).isoformat()}
        translated = sum(1 for a in articles if not a.get("translate"))
        print(f"  ✓ {cat} ({len(articles)} כתבות, {translated} מתורגמות)")

    with ThreadPoolExecutor(max_workers=4) as ex:
        list(ex.map(_initial_load, FEEDS.items()))
    print("הכל מוכן — מפעיל שרת")

    t = threading.Thread(target=refresh_cache, daemon=True)
    t.start()
    port = int(os.environ.get("PORT", 5000))
    app.run(debug=False, host="0.0.0.0", port=port)
