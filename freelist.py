from bs4 import BeautifulSoup
import sqlite3
import time
import sys
import os

from urllib.request import Request, urlopen

masterURL = "http://www.freelist.gr/"

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
		if cols:
			self.title = cols[3].find("b").text
			self.link = masterURL + cols[3].find("a")["href"]
			self.thumb = masterURL + cols[2].find("img")["src"]
			price = cols[4].text
			if price[0] == '€':
				self.price = float(price[2:])
			self.timeStamp = int(time.time())

def parseOffers(url, delay, pages):
	siteSrc = urlopen(Request(url, headers={'User-Agent': 'Mozilla/5.0'})).read()
	soup = BeautifulSoup(siteSrc.decode("utf-8", "ignore"), "lxml")
	table = soup.find("table", {"class": "tableResults"})

	rows = table.findAll("tr", {"onclick": "mark_row(this,1)"})

	offers = []
	for i in range(pages):
		for row in rows:
			newOffer = Offer(row)
			if newOffer.title != "":
				offers.append(newOffer)
		pageLinkTab = soup.find("div",{ "id": "pageNumbers"})
		pageLinks = pageLinkTab.findAll("a")
		nextPage = ""
		if pageLinks:
			nextPage = pageLinks[len(pageLinks)-1]
			if nextPage.text != " Επόμενη »":
				nextPage = ""

		time.sleep(delay)
		if not nextPage:
			break
		pages -= 1
		if pages <= 0:
			break

		sys.stdout.write('\'')
		sys.stdout.flush()

		siteSrc = urlopen(Request(url, headers={'User-Agent': 'Mozilla/5.0'})).read()
		soup = BeautifulSoup(siteSrc.decode("utf-8", "ignore"), "lxml")
		table = soup.find("table", {"class": "tableResults"})

		rows = table.findAll("tr", {"onclick": "mark_row(this,1)"})

	return offers

class Subcategory:

	def __init__(self, scTag, delay, pages):
		self.name = ""
		self.offers = []
		self.parse(scTag, delay, pages)

	def parse(self, scTag, delay, pages):
		self.name = scTag.text
		self.offers = parseOffers(masterURL + scTag["href"], delay, pages)

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
		scTags = cTag.findAll("a")
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

	def __init__(self):
		self.delay = 0.02 # Delay between website requests
		self.maxPages = 1 # Maximum Depth
		self.cats = []

		self.DBpath = "webCache.db"

	def update(self):
		self.cats = []
		siteSrc = urlopen(Request(masterURL, headers={'User-Agent': 'Mozilla/5.0'})).read()
		soup = BeautifulSoup(siteSrc.decode("utf-8", "ignore"), "lxml")
		cTags = soup.findAll("div", {"class":"category"})

		print("Update started.")
		print("URL: %s" % masterURL)
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

dBase = Database()
dBase.update()

dBase.export()
