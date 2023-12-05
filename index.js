const socket = io()
const html = document.querySelector('html')
const body = html.appendChild(document.createElement('body'))

const titleHeading = document.createElement('h1')
const entryHeading = document.createElement('h2')
const selectedHeading = document.createElement('h2')
const perfHeading = document.createElement('h2')

titleHeading.innerText = '🚀 RocketPerv 📉'
entryHeading.innerText = 'Enter Validators'
selectedHeading.innerText = 'Selected Validators'
perfHeading.innerText = 'Performance of Selected Validators'

const entityEntryBox = document.createElement('textarea')
entityEntryBox.placeholder = 'Node, minipool, or withdrawal addresses/ENS names, and/or validator pubkeys/indices, separated by spaces or commas'
entityEntryBox.cols = 42
entityEntryBox.rows = 5

const selectedEntitiesList = document.createElement('ul')

body.append(
  titleHeading,
  entryHeading,
  entityEntryBox,
  selectedHeading,
  selectedEntitiesList,
  perfHeading
)
