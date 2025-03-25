import { parse } from 'node-html-parser';
import fs from 'fs';
import { resolve } from 'path';
import * as read_last_lines from 'read-last-lines';

// const url = '/wiki/Tomato';
// const url = '/wiki/Tulip';
// const url = '/wiki/Tulipa_orphanidea'
const url = '/wiki/Pinus_nigra';
let num_plants = fs.readdirSync('./store/plants').length;

type SpeciesData = {
    hierarchy: [string, string][];
    binomial_name: string;
    url: string;
}

const scraped_urls = fs.readFileSync('./store/urls_scraped.txt').toString().split('\n');
const scraped_map = new Map<string, boolean>();
const to_scrap_urls = [...new Set(fs.readFileSync('./store/urls_to_scrape.txt').toString().split('\n'))];
const to_scrape_map = new Map<string, boolean>();

scraped_urls.forEach(url => scraped_map.set(url, true));
to_scrap_urls.forEach(url => to_scrape_map.set(url, true));

const scrape_url = async (url: string): Promise<string[]> => {
    if(scraped_map.has(url)) return [];

    const new_links = await fetch(`https://en.wikipedia.org${url}`)
        .then((response) => response.text())
        .then((html) => {
            const root = parse(html);
            const biota = root.querySelector('table.infobox.biota');
            const biota_rows = biota?.querySelectorAll('tr');
        
            const hierarchy = biota_rows?.map((row) => {
                const td_data = row.querySelectorAll('td');
                if(td_data.length !== 2) return null;
            
                const label = td_data[0].text.trim().toLocaleLowerCase().slice(0, -1);
                const value = td_data[1].text.trim().toLocaleLowerCase();
                return [label, value] as [string, string]
            }).filter(item => item !== null);
        
            const binomial_name = biota?.querySelector('span.binomial')?.text?.trim().toLocaleLowerCase();
            
            const links_on_page = root.querySelectorAll('a').map(link => link.getAttribute('href'))
                .filter(link => link !== null && link !== undefined)
                .filter(link => link.startsWith('/wiki/'))
                .filter(link => !link.includes('.JPG'))
                .filter(link => !link.includes('.jpg'))
                .filter(link => !link.includes('.JPEG'))
                .filter(link => !link.includes('.jpeg'))
                .filter(link => !link.includes('.PNG'))
                .filter(link => !link.includes('.png'))
                .filter(link => !link.includes('#'))
                .filter(link => !link.includes(':'))
        
            const is_plant = hierarchy?.find(pair => pair[0] === 'kingdom' && (pair[1].includes('plantae') || pair[1].includes('fungi')));
            const is_species_page = hierarchy && binomial_name && is_plant;
            if(is_species_page) {
                const species_data: SpeciesData = {
                    hierarchy: hierarchy,
                    binomial_name: binomial_name,
                    url: url
                }
                fs.writeFileSync(
                    `./store/plants/${binomial_name.replace(/\s+/g, '_').replace(/[^a-z._]/g, '')}.json`, 
                    JSON.stringify(species_data, null, 2)
                );
                num_plants++;
            }
        
            scraped_map.set(url, true);
            console.log(`# urls scraped: ${
                scraped_map.size.toString().padStart(8, ' ')
            }, # left: ${
                to_scrape_map.size.toString().padStart(8, ' ')
            }, # plants scraped: ${num_plants.toString().padStart(8, ' ')}`);

            return is_plant ? links_on_page : [];
        });

    return new_links;
}

// plants
// # urls scraped:   181167, # left:        1, # plants scraped:    60423

const sleep = async (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const run = async () => {
    while(to_scrape_map.size > 0){
        const [url, _] = to_scrape_map.entries().next().value;
        const new_urls = await scrape_url(url);
        to_scrape_map.delete(url);

        new_urls.forEach(url => {
            if(!scraped_map.has(url) && !to_scrape_map.has(url)) to_scrape_map.set(url, true);
        });

        const num_per_second = 10;
        const delta_factor = 0.8 + Math.random() * 0.4;
        await sleep(1000 / num_per_second * delta_factor);
    }
}
run();

setInterval(() => {
    fs.writeFileSync('./store/urls_to_scrape.txt', Array.from(to_scrape_map.keys()).join('\n'));
}, 5000);
setInterval(() => {
    fs.writeFileSync('./store/urls_scraped.txt', Array.from(scraped_map.keys()).join('\n'));
}, 5000);