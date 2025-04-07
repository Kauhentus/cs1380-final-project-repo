const readline = require('readline');

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
    prompt: 'search-engine> '
});

// Mock search function
function search(query) {
    const database = [
        'Node.js tutorial',
        'JavaScript basics',
        'How to build a search engine',
        'Understanding REPL in Node.js',
        'Search algorithms in computer science'
    ];

    return database.filter(item => item.toLowerCase().includes(query.toLowerCase()));
}

console.log('Welcome to the Nature Engine CLI!');
console.log('Use /help to get started.');
rl.prompt();

rl.on('line', (line) => {
    const input = line.trim();
    if(input[0] !== '/') {
        console.log('No command received.');
        console.log('Please use /help to see available commands.')
        return rl.prompt();
    }

    const args = input.slice(1).trim().split(' ');
    if (args[0] === 'exit') {
        console.log('Goodbye!');
        rl.close();
    } 

    else if (args[0] === 'help'){
        console.log('Available commands:');
        console.log('  /help                - Show this help message');
        console.log('  /search [keywords]   - Search for page with keywords');
        console.log('  /query  [keyword]    - Range query with taxonomy keyword');
        console.log('  /exit                - Exit the search CLI'); 
    }
    
    else if (args[0] === 'search') {
        const results = search(args.slice(1).join(' '));
        if (results.length > 0) {
            console.log('Search Results:');
            results.forEach((result, index) => {
                console.log(`${index + 1}. ${result}`);
            });
        } else {
            console.log('No search results found.');
        }
    } 

    else if (args[0] === 'query') {
        const results = search(args.slice(1).join(' '));
        if (results.length > 0) {
            console.log('Query Result:');
            results.forEach((result, index) => {
                console.log(`${index + 1}. ${result}`);
            });
        } else {
            console.log('No query result found.');
        }
    } 
    
    else {
        console.log('Please enter a valid search query.');
    }

    rl.prompt();
}).on('close', () => {
    process.exit(0);
});