import express from 'express'
import helmet from 'helmet'
import https from 'node:https' // TODO: use this when server is set up
import http from 'node:http'
import { Server } from 'socket.io'
import { dirname } from 'node:path'
import { fileURLToPath } from 'node:url'

const app = express()

app.use(helmet())

const options = {root: dirname(fileURLToPath(import.meta.url))}

app.get('/', (req, res) => {
  res.sendFile('index.html', options)
})

app.get('/index.js', (req, res) => {
  res.sendFile('index.js', options)
})

app.get('/index.css', (req, res) => {
  res.sendFile('index.css', options)
})

app.get('/icon.png', (req, res) => {
  res.sendFile('icon.png', options)
})

const server = http.createServer(app)
const io = new Server(server)
server.listen(3000)

io.on('connection', socket => {
  console.log(`connection: ${socket.id}`)
  socket.on('disconnect', () => {
    console.log(`disconnection: ${socket.id}`)
  })
})
