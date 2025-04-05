const fs = require('fs');
const path = require('path');

function renameFilesRecursive(dir) {
    fs.readdir(dir, (err, files) => {
        if (err) {
            console.error(`Error reading directory ${dir}: ${err}`);
            return;
        }

        files.forEach(file => {
            const filePath = path.join(dir, file);
            fs.stat(filePath, (err, stats) => {
                if (err) {
                    console.error(`Error reading file stats for ${filePath}: ${err}`);
                    return;
                }

                if (stats.isDirectory()) {
                    // Recursively process subdirectories
                    renameFilesRecursive(filePath);
                } else if (stats.isFile()) {
                    // Check if file has no extension
                    if (!path.extname(file)) {
                        const newFilePath = filePath + '.json';
                        fs.rename(filePath, newFilePath, (err) => {
                            if (err) {
                                console.error(`Error renaming file ${filePath}: ${err}`);
                            } else {
                                console.log(`Renamed: ${filePath} -> ${newFilePath}`);
                            }
                        });
                    }
                }
            });
        });
    });
}

// Start the recursive renaming process in the './store' directory
renameFilesRecursive('./store');