import express from 'express'
import helmet from 'helmet'
import https from 'node:https' // TODO: use this when server is set up
import http from 'node:http'
import { Server } from 'socket.io'

const app = express()

app.use(helmet())

app.get('/', (req, res) => {
  res.send('hello world')
})


const server = http.createServer(app)
const io = new Server(server)
server.listen(3000)
