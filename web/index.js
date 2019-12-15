var net = require('net');

var HOST = '0.0.0.0';
var PORT = 4321;

var socks = new Set();

net.createServer(function (sock) {

  socks.add(sock);

  console.log('CONNECTED: ' + sock.remoteAddress + ':' + sock.remotePort);

  sock.on('data', function (data) {
    console.log('DATA ' + sock.remoteAddress + ': ' + data);

    // sock.write('You said "' + data + '"');
  });

  sock.on('close', function (data) {
    console.log('CLOSED: ' + sock.remoteAddress + ' ' + sock.remotePort);
    socks.delete(sock)
  });

}).listen(PORT, HOST);

console.log('Server listening on ' + HOST + ':' + PORT);

var standard_input = process.stdin;
standard_input.setEncoding('utf-8');

console.log("Please input text in command line.");

// When user input data and click enter key.
standard_input.on('data', function (data) {

    // User input exit.
    if(data === 'exit\n'){
        process.exit();
    }else
    {
      socks.forEach(sock => {
        sock.write(data);
      });
    }
});
