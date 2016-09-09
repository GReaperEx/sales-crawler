from bs4 import BeautifulSoup
import requests
import sqlite3
import time
import sys
import os

class Offer:

	def __init__(self, row):
		self.title = ""
		self.link = ""
		self.thumb = ""
		self.price = 0.0
		self.uName = ""
		self.uLink = ""
		self.timeStamp = 0

		self.parse(row)

	def parse(self, row):
		cols = row.findAll("td")
		lock = row.find("img", {"src":"http://www.insomnia.gr/public/style_images/master/classifieds/lock.png"})
		if cols and not lock:
			self.title = cols[1].find("a").text
			self.link = cols[1].find("a")["href"]
			self.thumb = cols[0].find("img")["src"]
			price = cols[3].text
			if price[0] == '€':
				self.price = float(price[1:])
			self.uName = cols[2].find("a").text
			self.uLink = cols[2].find("a")["href"]
			self.timeStamp = int(time.time())

def parseOffers(url, delay, pages):
	siteSrc = requests.get(url).text
	soup = BeautifulSoup(siteSrc, "lxml")
	table = soup.find("table")

	rows = table.findAll("tr")

	offers = []
	for i in range(pages):
		for row in rows:
			if row["class"] != "header":
				newOffer = Offer(row)
				if newOffer.title != "":
					offers.append(newOffer)
		nextPage = soup.find("a", {"title":"Επόμενη Σελίδα"})

		time.sleep(delay)
		if not nextPage:
			break
		pages -= 1
		if pages <= 0:
			break

		sys.stdout.write('\'')
		sys.stdout.flush()

		siteSrc = requests.get(nextPage["href"]).text
		soup = BeautifulSoup(siteSrc, "lxml")
		table = soup.find("table")

		rows = table.findAll("tr")

	return offers

class Subcategory:

	def __init__(self, scTag, delay, pages):
		self.name = ""
		self.offers = []
		self.parse(scTag, delay, pages)

	def parse(self, scTag, delay, pages):
		tag = scTag.find("a")
		self.name = tag.text
		self.offers = parseOffers(tag["href"], delay, pages)

	def __str__(self):
		result = ("=====%s=====\n" % self.name)
		for offer in self.offers:
			result += offer.__str__() + "\n\n"

		return result

class Category:

	def __init__(self, cTag, delay, pages):
		self.name = ""
		self.subcats = []
		self.parse(cTag, delay, pages)

	def parse(self, cTag, delay, pages):
		self.name = cTag.find("a").text

		scTags = cTag.findAll("li", {"class":"subcategory"})
		for tag in scTags:
			self.subcats.append(Subcategory(tag, delay, pages))
			sys.stdout.write('.')
			sys.stdout.flush()

	def __str__(self):
		result = ("==%s==\n" % self.name)
		for sc in self.subcats:
			result += sc.__str__()

		return result

class Database:

	def __init__(self, url):
		self.masterURL = url

		self.delay = 0.02 # Delay between website requests
		self.maxPages = 1 # Maximum Depth
		self.cats = []

		self.DBpath = "webCache.db"

	def update(self):
		self.cats = []
		siteSrc = requests.get(self.masterURL).text
		soup = BeautifulSoup(siteSrc, "lxml")
		cTags = soup.findAll("li", {"class":"category"})

		print("Update started.")
		print("URL: %s" % self.masterURL)
		print("Delay: %.3f seconds" % self.delay)
		print("Max Depth: %d pages" % self.maxPages)
		for tag in cTags:
			self.cats.append(Category(tag, self.delay, self.maxPages))
			sys.stdout.write('|')
			sys.stdout.flush()
		print("\nDone.")

	def export(self):
		print("Export started.")
		conn = sqlite3.connect(self.DBpath)
		cursor = conn.cursor()

		cursor.execute("""CREATE TABLE IF NOT EXISTS
						  categories(id INTEGER PRIMARY KEY, name TEXT)""")
		cursor.execute("""CREATE TABLE IF NOT EXISTS
						  subcategories(id INTEGER PRIMARY KEY, name TEXT, catID INTEGER)""")
		cursor.execute("""CREATE TABLE IF NOT EXISTS
						  offers(title TEXT, link TEXT, thumb TEXT, price REAL, uName TEXT, uLink TEXT, catID INTEGER, subcatID INTEGER, timestamp INTEGER)""")

		for cat in self.cats:
			cursor.execute("""SELECT id FROM categories WHERE name=?""", [cat.name])
			catRow = cursor.fetchone()
			catID = 0
			if catRow:
				catID = catRow[0]
			else:
				cursor.execute("""INSERT INTO categories(name) VALUES(?)""", [cat.name])
				cursor.execute("""SELECT id FROM categories WHERE name=?""", [cat.name])
				catID = cursor.fetchone()[0]

			for subcat in cat.subcats:
				cursor.execute("""SELECT id FROM subcategories WHERE name=? AND catID=?""", [subcat.name, catID])
				subcatRow = cursor.fetchone()
				if subcatRow:
					subcatID = subcatRow[0]
				else:
					cursor.execute("""INSERT INTO subcategories(name, catID) VALUES(?, ?)""", [subcat.name, catID])
					cursor.execute("""SELECT id FROM subcategories WHERE name=? AND catID=?""", [subcat.name, catID])
					subcatID = cursor.fetchone()[0]

				for offer in subcat.offers:
					cursor.execute("""SELECT * FROM offers WHERE link=?""", [offer.link])
					dbRow = cursor.fetchone()
					if not dbRow:
						cursor.execute("""INSERT INTO offers
										  VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)""",
										  [offer.title, offer.link, offer.thumb, offer.price, offer.uName, offer.uLink, catID, subcatID, offer.timeStamp])

				sys.stdout.write('.')
				sys.stdout.flush()
			sys.stdout.write('|')
			sys.stdout.flush()

		conn.commit()
		conn.close()

		print("\nDone.")

	def __str__(self):
		result = ""
		for c in self.cats:
			result += c.__str__()

		return result

""" MAIN PROGRAM """

dBase = Database("http://www.insomnia.gr/classifieds/")
dBase.update()

dBase.export()
