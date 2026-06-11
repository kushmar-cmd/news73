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
        ("Ynet חדשות", "https://www.ynet.co.il/Integration/StoryRss2.xml"),
        ("Walla חדשות", "https://rss.walla.co.il/feed/1"),
        ("מאקו N12", "https://www.mako.co.il/rss/news.xml"),
        ("הארץ", "https://www.haaretz.co.il/cmlink/1.1660017"),
    ],
    "פוליטיקה ישראלית": [
        ("Ynet פוליטי", "https://www.ynet.co.il/Integration/StoryRss2030.xml"),
        ("Walla פוליטי", "https://rss.walla.co.il/feed/6"),
        ("BBC Politics", "http://feeds.bbci.co.uk/news/politics/rss.xml"),
        ("Politico", "https://www.politico.com/rss/politics08.xml"),
    ],
    "עולם": [
        ("BBC World", "http://feeds.bbci.co.uk/news/world/rss.xml"),
        ("Al Jazeera", "https://www.aljazeera.com/xml/rss/all.xml"),
        ("Guardian World", "https://www.theguardian.com/world/rss"),
        ("Ynet עולם", "https://www.ynet.co.il/Integration/StoryRss377.xml"),
    ],
    "כלכלה": [
        ("Ynet כלכלה", "https://www.ynet.co.il/Integration/StoryRss3.xml"),
        ("Walla כלכלה", "https://rss.walla.co.il/feed/2"),
        ("BBC Business", "http://feeds.bbci.co.uk/news/business/rss.xml"),
        ("Reuters Business", "https://feeds.reuters.com/reuters/businessNews"),
    ],
    "טכנולוגיה": [
        ("Ynet טכנולוגיה", "https://www.ynet.co.il/Integration/StoryRss542.xml"),
        ("Walla טכנולוגיה", "https://rss.walla.co.il/feed/4"),
        ("TechCrunch", "https://techcrunch.com/feed/"),
        ("The Verge", "https://www.theverge.com/rss/index.xml"),
        ("Wired", "https://www.wired.com/feed/rss"),
    ],
    "ספורט": [
        ("Ynet ספורט", "https://www.ynet.co.il/Integration/StoryRss5.xml"),
        ("Walla ספורט", "https://rss.walla.co.il/feed/3"),
        ("BBC Sport", "http://feeds.bbci.co.uk/sport/rss.xml"),
        ("ESPN", "https://www.espn.com/espn/rss/news"),
    ],
    "בידור ותרבות": [
        ("Ynet בידור", "https://www.ynet.co.il/Integration/StoryRss4.xml"),
        ("Walla בידור", "https://rss.walla.co.il/feed/7"),
        ("BBC Entertainment", "http://feeds.bbci.co.uk/news/entertainment_and_arts/rss.xml"),
        ("Rolling Stone", "https://www.rollingstone.com/feed/"),
    ],
    "בריאות": [
        ("Ynet בריאות", "https://www.ynet.co.il/Integration/StoryRss3458.xml"),
        ("Walla בריאות", "https://rss.walla.co.il/feed/5"),
        ("BBC Health", "http://feeds.bbci.co.uk/news/health/rss.xml"),
        ("WHO", "https://www.who.int/rss-feeds/news-releases.xml"),
    ],
    "מדע וטבע": [
        ("Ynet מדע", "https://www.ynet.co.il/Integration/StoryRss3462.xml"),
        ("Science Daily", "https://www.sciencedaily.com/rss/all.xml"),
        ("NASA", "https://www.nasa.gov/rss/dyn/breaking_news.rss"),
        ("New Scientist", "https://www.newscientist.com/feed/home/"),
    ],
    "סביבה ואקלים": [
        ("BBC Environment", "http://feeds.bbci.co.uk/news/science_and_environment/rss.xml"),
        ("Guardian Environment", "https://www.theguardian.com/environment/rss"),
        ("Climate Home News", "https://www.climatechangenews.com/feed/"),
    ],
    "חינוך": [
        ("BBC Education", "http://feeds.bbci.co.uk/news/education/rss.xml"),
        ("Times Higher Education", "https://www.timeshighereducation.com/news/rss.xml"),
        ("EdSurge", "https://www.edsurge.com/news.rss"),
    ],
}

cache = {}
cache_lock = threading.Lock()
CACHE_TTL = 300  # 5 minutes


def parse_rss(xml_text, source_name):
    items = []
    try:
        root = ET.fromstring(xml_text)
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        # Standard RSS
        for item in root.findall(".//item")[:8]:
            title = item.findtext("title", "").strip()
            link = item.findtext("link", "").strip()
            desc = item.findtext("description", "").strip()
            pub = item.findtext("pubDate", "").strip()
            if title and link:
                items.append({"title": title, "link": link, "desc": _clean(desc)[:200], "pub": pub, "source": source_name})
        # Atom feeds
        if not items:
            for entry in root.findall(".//atom:entry", ns)[:8]:
                title = entry.findtext("atom:title", "", ns).strip()
                link_el = entry.find("atom:link", ns)
                link = link_el.get("href", "") if link_el is not None else ""
                summary = entry.findtext("atom:summary", "", ns).strip()
                pub = entry.findtext("atom:updated", "", ns).strip()
                if title and link:
                    items.append({"title": title, "link": link, "desc": _clean(summary)[:200], "pub": pub, "source": source_name})
    except Exception:
        pass
    return items


def _clean(text):
    import re
    text = re.sub(r"<[^>]+>", "", text)
    return text.strip()


def fetch_feed(source_name, url):
    try:
        resp = requests.get(url, timeout=8, headers={"User-Agent": "Mozilla/5.0 (compatible; NewsApp/1.0)"})
        if resp.status_code == 200:
            # fix encoding for Hebrew sites
            if resp.encoding and resp.encoding.lower() in ("iso-8859-1", "windows-1252"):
                resp.encoding = resp.apparent_encoding
            return parse_rss(resp.text, source_name)
    except Exception:
        pass
    return []


def fetch_category(category, feeds):
    articles = []
    with ThreadPoolExecutor(max_workers=6) as ex:
        futures = {ex.submit(fetch_feed, name, url): name for name, url in feeds}
        for f in as_completed(futures, timeout=12):
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
            with cache_lock:
                cache[cat] = {"articles": articles, "updated": datetime.now(timezone.utc).isoformat()}

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
        data = {"articles": articles, "updated": datetime.now(timezone.utc).isoformat()}
        with cache_lock:
            cache[category] = data
    return jsonify(data)


if __name__ == "__main__":
    t = threading.Thread(target=refresh_cache, daemon=True)
    t.start()
    app.run(debug=False, port=5000)
