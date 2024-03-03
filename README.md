# Address Parser

This project is a Python application that parses addresses from a list of URLs. <br>
It was made for the role of "Deeptech Engineer" Intern at <a href="https://veridion.com/" target="_blank">Veridion</a>

## The given script
Write a program that extracts all the valid addresses that are found on a list of company websites. The format in which you will have to extract this data is the following: country, region, city, postcode, road, and road numbers. 

## My thought process

Firstly we'll have to fetch all the pages. But this gets us only to the home page, where the address might not be present.
Usually, an company's address is present either on the contact page or the about page of the company. <br>

After fetching the main page, and parsing it using BeautifulSoup with LXML parser for a faster parsing experience, we check all the 'href' attributes of the link tags, and if it contains 'contact' or 'about', we append it to a list. <br>

We also have to take in consideration that some 'href' attributes might not be links, but mails, so we filter them as well using conditions.


## Installation

1. Clone this repository:
    ```
    git clone https://github.com/steffegit/VeridionAssignment.git
    ```
2. Navigate to the project directory:
    ```
    cd VeridionAssignment
    ```
3. Install the required dependencies:
    ```
    pip install -r requirements.txt
    ```

## Usage

Run the main script:
  ```
  python3 main.py
  ```


## Features

- Parses addresses from a list of URLs.
- Uses BeautifulSoup for HTML parsing (with LXML parser)
- Uses geopy for geolocation.