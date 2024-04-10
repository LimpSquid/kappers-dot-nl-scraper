use std::collections::HashSet;
use std::error::Error;
use std::future::Future;
use std::path::PathBuf;
use std::time::Duration;

use chrono::Utc;
use html_escape::decode_html_entities;
use scraper::{Html, Selector};
use serde::Serialize;
use tokio::fs::{create_dir_all, File};
use tokio::io::AsyncWriteExt;

const USER_AGENT: &str = "scrapy mc scrapeface, contact me at git@limpsquid.nl";
const BASE_URL: &str = "https://kappers.nl";
const REGIONS: &[(&str, &str)] = &[
    ("noord-holland", "noord-holland"),
    ("zuid-holland", "zuid-holland"),
    ("drenthe", "drenthe"),
    ("flevoland", "flevoland"),
    ("friesland", "friesland"),
    ("gelderland", "gelderland"),
    ("groningen", "groningen-provincie"),
    ("limburg", "limburg"),
    ("noord-brabant", "noord-brabant"),
    ("overijssel", "overijssel"),
    ("utrecht", "utrecht-provincie"),
    ("zeeland", "zeeland-provincie"),
];

#[derive(Debug, Hash, PartialEq, Eq, Serialize)]
struct SalonDetails {
    name: String,
    street_address: String,
    postal_code: String,
    locality: String,
    region: String,
    latitude: String,
    longitude: String,
}

#[tokio::main]
async fn main() {
    let root_dir = PathBuf::from(format!("./{}", Utc::now().date_naive()));
    create_dir_all(&root_dir).await.unwrap();

    for (region, region_slug) in REGIONS {
        let scraper = || async { scrape_region(region, region_slug, root_dir.clone()).await };
        match with_retries(scraper).await {
            Ok(_) => println!("Scraped region: {}", region),
            Err(err) => println!("Failed to scrape region: {}, error: {}", region, err),
        };
    }
}

async fn scrape_region(
    region: &str,
    region_slug: &str,
    root_dir: PathBuf,
) -> Result<(), Box<dyn Error>> {
    let client = reqwest::Client::builder().user_agent(USER_AGENT).build()?;
    let filepath = root_dir.join(format!("{}.json", region));
    let body = client
        .get(format!("{}/{}", BASE_URL, region_slug))
        .send()
        .await?
        .text()
        .await?;
    let document = Html::parse_document(&body);
    let selector_province = Selector::parse("div.row.province-list.alphabetic")?;
    let selector_city_link = Selector::parse("a")?;

    let mut salon_details = HashSet::new();
    let links = document
        .select(&selector_province)
        .next()
        .ok_or("Unable to select province")?
        .select(&selector_city_link);

    for link in links {
        let page_url = match link.attr("href") {
            Some(href) => format!("{}{}", BASE_URL, href),
            None => {
                println!("Failed to get href for element: {}", link.html());
                continue;
            }
        };
        let scraper = || async { scrape_page(&page_url).await };
        let details = match with_retries(scraper).await {
            Ok(details) => details,
            Err(err) => {
                println!("Failed to scrape page: {}, error: {}", page_url, err);
                continue;
            }
        };

        println!("Found {} unique salons at {}", details.len(), page_url);
        salon_details.extend(details);

        // Write everything to the file every iteration
        let mut file = File::create(filepath.clone()).await?;
        let contents = serde_json::to_string_pretty(&salon_details)?;
        file.write(contents.as_bytes()).await?;
        file.flush().await?;

        // Be nice and do not spam the website
        // sleep(Duration::from_secs(2));
    }

    Ok(())
}

async fn scrape_page(page_url: &str) -> Result<HashSet<SalonDetails>, Box<dyn Error>> {
    let client = reqwest::Client::builder().user_agent(USER_AGENT).build()?;
    let body = client.get(page_url).send().await?.text().await?;
    let document = Html::parse_document(&body);
    let selector_salon = Selector::parse("div.listing-details.d-flex.flex-column")?;
    let selector_name = Selector::parse(r#"span[itemprop="name"]"#).unwrap();
    let selector_street_address = Selector::parse(r#"span[itemprop="streetAddress"]"#)?;
    let selector_postal_code = Selector::parse(r#"span[itemprop="postalCode"]"#)?;
    let selector_locality = Selector::parse(r#"span[itemprop="addressLocality"]"#)?;
    let selector_region = Selector::parse(r#"meta[itemprop="addressRegion"]"#)?;
    let selector_latitude = Selector::parse(r#"meta[itemprop="latitude"]"#)?;
    let selector_longitude = Selector::parse(r#"meta[itemprop="longitude"]"#)?;

    let mut result = HashSet::new();
    for element in document.select(&selector_salon) {
        let name = element
            .select(&selector_name)
            .next()
            .map(|x| x.inner_html())
            .unwrap_or_default();
        let street_address = element
            .select(&selector_street_address)
            .next()
            .map(|x| x.inner_html())
            .unwrap_or_default();
        let postal_code = element
            .select(&selector_postal_code)
            .next()
            .map(|x| x.inner_html())
            .unwrap_or_default();
        let locality = element
            .select(&selector_locality)
            .next()
            .map(|x| x.inner_html())
            .unwrap_or_default();
        let region = element
            .select(&selector_region)
            .next()
            .map(|x| x.attr("content"))
            .flatten()
            .unwrap_or_default();
        let latitude = element
            .select(&selector_latitude)
            .next()
            .map(|x| x.attr("content"))
            .flatten()
            .unwrap_or_default();
        let longitude = element
            .select(&selector_longitude)
            .next()
            .map(|x| x.attr("content"))
            .flatten()
            .unwrap_or_default();

        result.insert(SalonDetails {
            name: decode_html_entities(&name).into_owned(),
            street_address: decode_html_entities(&street_address).into_owned(),
            postal_code: decode_html_entities(&postal_code).into_owned(),
            locality: decode_html_entities(&locality).into_owned(),
            region: decode_html_entities(&region).into_owned(),
            latitude: decode_html_entities(&latitude).into_owned(),
            longitude: decode_html_entities(&longitude).into_owned(),
        });
    }

    Ok(result)
}

async fn with_retries<F, O>(func: impl Fn() -> F) -> Result<O, Box<dyn Error>>
where
    F: Future<Output = Result<O, Box<dyn Error>>>,
{
    use tokio::time::sleep;

    let mut retries = 3;

    loop {
        match func().await {
            Ok(result) => break Ok(result),
            Err(_) if retries > 0 => {
                retries -= 1;
                sleep(Duration::from_secs(5)).await;
            }
            Err(err) => break Err(err),
        }
    }
}
