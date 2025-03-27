# M0: Setup & Centralized Computing

> Add your contact information below and in `package.json`.

* name: `Armaan Patankar`

* email: `armaan_patankar@brown.edu`

* cslogin: `ampatank`


## Summary

This was a challenging first assignment for the course. I got a good grasp of how to work with JavaScript and shell. It was interesting to work with the different constructs in this language. I found it hard to really thoroughly write shell tests and make sure that they tested all of the inputs. 

My implmentation consists of 7 JavaScript components and 1 Shell component.
<ul>
<li>`getText.js` : This JS file is responsible with obtaining the plaintext from the HTML file
<li>`getURLs.js` : This JS file is responsible with obtaining the links within each page to further crawl
<li>`merge.js` : This JS file is responsible with merging the local index with the global index (hardest to implement)
<li>`process.sh` : This shell script is responsible with processing the text into a useable and functional form
<li>`stem.js : This JS file is responsible for obtaining the stem of the word

The most challenging aspect for me testing. It was hard to find inputs that thoroughly test the files and understanding the syntax took a minute. It was also a hard to write the functions/files to test the throughput of my functions. 

## Correctness & Performance Characterization

I characterized correctness by writing tests for `combine.sh`, `getText.js`, `getURLs.js`, `invert.sh`, `merge.js`, `process.sh`, `query.js`, and `stem.js`. These tests includes an input file that has various test cases to test the correctness and functionality of the methods. I specifically tried to use edge case inputs to ensure they work properly.

*Performance*: I characterized performance using two files. I used the `throuhgput.sh` file to test the `crawler` and `indexer`. This file runs the crawler and indexer on a set corpus and times them. Once done, it measures how long it took them to run. This is then used to obtain the throughput of pages/sec. For the querier, I randomly sampled words/phrases from the global index and ran the querier. I then measured how long it took.

## Wild Guess

I think it will likely take 30000 lines of code. While that number is large, I think that this class will require a lot of infrastructure code that will require a lot out of us. 