const socket = io()
const frag = document.createDocumentFragment()
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

entityEntryBox.addEventListener('change',
  () => socket.emit('entities', entityEntryBox.value)
  ,{passive: true})

const selectedEntitiesList = document.createElement('ul')

socket.on('minipools', minipools => {
  for (const {minipoolAddress, minipoolEnsName, nodeAddress, nodeEnsName, validatorIndex, selected} of minipools) {
    const li = frag.appendChild(document.createElement('li'))
    const mpA = document.createElement('a')
    mpA.href = `https://rocketscan.io/minipool/${minipoolAddress}`
    mpA.innerText = minipoolEnsName || minipoolAddress
    const nodeA = document.createElement('a')
    nodeA.href = `https://rocketscan.io/node/${nodeAddress}`
    nodeA.innerText = nodeEnsName || nodeAddress
    const valA = document.createElement('a')
    valA.href = `https://beaconcha.in/validator/${validatorIndex}`
    valA.innerText = validatorIndex
    const sel = document.createElement('input')
    sel.type = 'checkbox'
    sel.checked = selected
    li.append(
      document.createTextNode('minipool: '), mpA,
      document.createTextNode(' (node: '), nodeA, document.createTextNode(') '),
      document.createTextNode(' (validator: '), valA, document.createTextNode(') '),
      sel
    )
  }
  selectedEntitiesList.replaceChildren()
  selectedEntitiesList.appendChild(frag)
})

body.append(
  titleHeading,
  entryHeading,
  entityEntryBox,
  selectedHeading,
  selectedEntitiesList,
  perfHeading
)
