const http = require("http");
const fs = require("fs");

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
}).listen(8080, () => console.log("[+] Server is running..."));