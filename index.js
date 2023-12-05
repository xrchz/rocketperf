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
entityEntryBox.cols = 96
entityEntryBox.rows = 6

entityEntryBox.addEventListener('change',
  () => socket.emit('entities', entityEntryBox.value)
  ,{passive: true})

const minipoolsList = document.createElement('table')
const headings = ['Minipool', 'Node', 'Validator', 'Include']
minipoolsList.appendChild(document.createElement('tr'))
  .append(
    ...headings.map(h => {
      const th = document.createElement('th')
      th.innerText = h
      return th
    })
  )

socket.on('minipools', minipools => {
  for (const {minipoolAddress, minipoolEnsName, nodeAddress, nodeEnsName, validatorIndex, selected} of minipools) {
    const tr = frag.appendChild(document.createElement('tr'))
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
    tr.append(
      ...[mpA, nodeA, valA, sel].map(a => {
        const td = document.createElement('td')
        td.appendChild(a)
        return td
      })
    )
    mpA.parentElement.classList.add('minipool')
    if (!minipoolEnsName) mpA.parentElement.classList.add('address')
    nodeA.parentElement.classList.add('node')
    if (!nodeEnsName) nodeA.parentElement.classList.add('address')
    valA.parentElement.classList.add('validator')
    sel.parentElement.classList.add('selected')
  }
  minipoolsList.replaceChildren(minipoolsList.firstElementChild)
  minipoolsList.appendChild(frag)
})

body.append(
  titleHeading,
  entryHeading,
  entityEntryBox,
  selectedHeading,
  minipoolsList,
  perfHeading
)
