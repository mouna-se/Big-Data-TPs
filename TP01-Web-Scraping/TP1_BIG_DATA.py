import requests
from bs4 import BeautifulSoup
import csv
import re

# Clean text from extra spaces and line breaks
def clean_text(text):
    if not text:
        return ""
    text = text.replace("\n", " ").replace("\r", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


# Connect to the main website page
url = "https://vitamine.dz"
response = requests.get(url)
soup = BeautifulSoup(response.text, "html.parser")
print("Status code:", response.status_code)


# Extract unique book URLs
links = soup.find_all("a")
print("Number of links found:", len(links))

book_urls = []
for link in links:
    href = link.get("href")
    if href and "/fr/" in href and href.endswith(".php"):
        if href not in book_urls:
            book_urls.append(href)

print("Number of unique books to scrape:", len(book_urls))


# Create the CSV file
with open("vitamine_books.csv", "w", newline="", encoding="utf-8-sig") as file:
    writer = csv.writer(file, delimiter=";", quoting=csv.QUOTE_MINIMAL)
    
    writer.writerow(["ID", "Title", "Category", "Price", "Availability", "Book URL"])

    # Scrape each book page
    for i, book_url in enumerate(book_urls, start=1):
        try:
            response = requests.get(book_url, timeout=20)
            soup_book = BeautifulSoup(response.text, "html.parser")
            
            section = soup_book.find("div", class_="section group")
            
            title = ""
            category = ""
            price = ""
            availability = ""

            if section:
                # Extract book title
                title_element = section.find("h1")
                if title_element:
                    title = clean_text(title_element.get_text(" ", strip=True))

                # Extract book category
                content = section.find(
                    "div",
                    style=lambda value: value and "min-height: 400px" in value
                )
                if content:
                    first_text = next(content.stripped_strings, "")
                    if ":" in first_text:
                        category = clean_text(first_text.split(":", 1)[1])

                # Extract and convert book price
                price_element = section.find(
                    "div",
                    style=lambda value: value and "color:#CC00CE" in value
                )
                if price_element:
                    price_text = clean_text(price_element.get_text(" ", strip=True))
                    price_text = re.sub(
                        r"^Prix\s*:\s*",
                        "",
                        price_text,
                        flags=re.IGNORECASE
                    )
                    price_text = clean_text(price_text)

                    if "€" in price_text:
                        price_number = price_text.replace("€", "").strip()
                        price_number = float(price_number.replace(",", "."))
                        price_number *= 100

                        if price_number.is_integer():
                            price_number = int(price_number)

                        price = f"{price_number} DA"
                    else:
                        price = price_text

                # Extract book availability
                availability_image = section.find(
                    "img",
                    src=lambda value: value and (
                        "ajouter_au_panier_fr.png" in value or
                        "non_disponible_fr.png" in value
                    )
                )

                if availability_image:
                    src = availability_image.get("src", "")

                    if "ajouter_au_panier_fr.png" in src:
                        availability = "Available"
                    elif "non_disponible_fr.png" in src:
                        availability = "Not available"

            # Save the scraped data to the CSV file
            writer.writerow([
                i,
                title,
                category,
                price,
                availability,
                book_url
            ])

            print(f"Scraped ({i}/{len(book_urls)}): {title}")

        except Exception as e:
            print(f"Error scraping {book_url}: {e}")

print("CSV file created successfully!")