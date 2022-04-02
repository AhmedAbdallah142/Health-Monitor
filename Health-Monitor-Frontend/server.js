const http = require("http");
const fs = require("fs");
const port = 8085

http.createServer((req, res) => {
    if(req.url === "/") {
        fs.readFile("./src/index.html", (err, cont) => {
            res.writeHead(200, {"Content-Type": "text/html"})
            res.end(cont);
        })
    }
    else {
        fs.readFile(`./src${req.url}`, (err, cont) => {
            if(err) {
                res.writeHead(404);
                res.end();
            }
            else
                res.end(cont);
        })
    }
}).listen(port, () => console.log(`[+] Server is running on port ${port}`));