from bs4 import BeautifulSoup
import requests
import sqlite3
import time
import sys
import os

def cropTitle(str):
	start = 0
	end = len(str)-1

	for c in str:
		if not c.isspace():
			break
		start += 1

	for i in range(end, 0, -1):
		if not str[i].isspace() and not str[i].isdigit():
			break
		end -= 1

	return str[start:end+1:]

def cropPrice(str):
	start = 0
	end = len(str)-1

	for c in str:
		if not c.isspace():
			break
		start += 1

	isPrice = False

	for i in range(end, 0, -1):
		if not str[i].isspace() and str[i] != '€':
			break
		if str[i] == '€':
			isPrice = True

		end -= 1

	if isPrice:
		return str[start:end+1:].replace('.', '').replace(',', '.').replace(' ', '')

	return str[start:end+1:]

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
		itemInfo = row.find("div", {"class":"item-info"})

		linkTag = itemInfo.find("a", {"class":"history"})
		self.title = cropPrice(linkTag.text)
		self.link = linkTag["href"]
		if row.find("img"):
			self.thumb = row.find("img")["src"]
		#print(cropPrice(itemInfo.find("div", {"class":"item-params"}).text))
		priceStr = cropPrice(itemInfo.find("div", {"class":"item-params"}).text)
		if len(priceStr) > 0:
			self.price = float(priceStr)

		self.timeStamp = int(time.time())

def parseOffers(url, delay, pages):
	siteSrc = requests.get(url).text
	soup = BeautifulSoup(siteSrc, "lxml")
	table = soup.find("div", {"class": "listing listing-thumbs listing-thumbs-big"})

	rows = table.findAll("div", {"class":"item relative"})

	offers = []
	for i in range(pages):
		for row in rows:
			newOffer = Offer(row)
			if newOffer.title != "":
				offers.append(newOffer)
		nextPage = soup.find("a", {"class":"btn btn-right"})

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
		table = soup.find("div", {"class": "listing listing-thumbs listing-thumbs-big"})

		rows = table.findAll("div", {"class":"item relative"})

	return offers

class Subcategory:

	def __init__(self, scTag, delay, pages):
		self.name = ""
		self.offers = []
		self.parse(scTag, delay, pages)

	def parse(self, scTag, delay, pages):

		tag = scTag.find("a")
		self.name = cropTitle(tag.find("div", {"class": "ctext3"}).text)
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
		self.name = cTag.find("span").text

		scTags = cTag.findAll("li", {"class":"searchbox-subcategory"})
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
		cTags = soup.findAll("li", {"class":"searchbox-category"})

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

dBase = Database("http://www.aggeliopolis.gr/ellada/")
dBase.update()

dBase.export()
