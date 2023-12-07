const socket = io()
const frag = document.createDocumentFragment()
const html = document.querySelector('html')
const body = html.appendChild(document.createElement('body'))

const titleHeading = document.createElement('h1')
const entryHeading = document.createElement('h2')
const selectedHeading = document.createElement('h2')
const perfHeading = document.createElement('h2')
const slotsHeading = document.createElement('h3')
const summaryHeading = document.createElement('h3')
const detailsHeading = document.createElement('h3')

titleHeading.innerText = '🚀 RocketPerv 📉'
entryHeading.innerText = 'Enter Validators'
selectedHeading.innerText = 'Selected Validators'
perfHeading.innerText = 'Performance of Selected Validators'
slotsHeading.innerText = 'Time Range'
summaryHeading.innerText = 'Summary'
detailsHeading.innerText = 'Details'

const entityEntryBox = document.createElement('textarea')
entityEntryBox.placeholder = 'Node, minipool, or withdrawal addresses/ENS names,'.concat(
  ' and/or validator pubkeys/indices, separated by spaces and/or commas.',
  ' Put a * after a withdrawal address to include nodes it was ever previously for.'
)
entityEntryBox.title = entityEntryBox.placeholder
entityEntryBox.cols = 96
entityEntryBox.rows = 6
entityEntryBox.autocomplete = 'on'

entityEntryBox.addEventListener('change',
  () => socket.emit('entities', entityEntryBox.value),
  {passive: true}
)

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

const slotSelectionDiv = document.createElement('div')
const fromDatetimeLabel = document.createElement('label')
const fromDatetime = document.createElement('input')
const toDatetimeLabel = document.createElement('label')
const toDatetime = document.createElement('input')
// const timezoneLabel = document.createElement('label') TODO: add this later?
const fromSlotLabel = document.createElement('label')
const fromSlot = document.createElement('input')
const toSlotLabel = document.createElement('label')
const toSlot = document.createElement('input')
const slotSelectors = new Map()
slotSelectors.set('fromDatetime', fromDatetime)
slotSelectors.set('toDatetime', toDatetime)
slotSelectors.set('fromSlot', fromSlot)
slotSelectors.set('toSlot', toSlot)

slotSelectionDiv.id = 'slotSelection'

fromDatetime.type = 'datetime-local'
toDatetime.type = 'datetime-local'
fromSlot.type = 'number'
toSlot.type = 'number'
fromDatetime.dataset.dir = 'from'
fromSlot.dataset.dir = 'from'
toDatetime.dataset.dir = 'to'
toSlot.dataset.dir = 'to'
fromSlot.min = 0
toSlot.min = 0
fromSlotLabel.append(
  document.createTextNode('From slot: '),
  fromSlot
)
toSlotLabel.append(
  document.createTextNode('To slot: '),
  toSlot
)
fromDatetimeLabel.append(
  document.createTextNode('From time: '),
  fromDatetime
)
toDatetimeLabel.append(
  document.createTextNode('To time: '),
  toDatetime
)

const slotSelectionHandler = (e) =>
  socket.emit('setSlot',
    {dir: e.target.dataset.dir,
     type: e.target.type,
     value: e.target.value}
  )

slotSelectors.forEach(e =>
  e.addEventListener('change', slotSelectionHandler, {passive: true})
)

socket.on('setSlot', (key, value) =>
  slotSelectors.get(key).value = value
)

slotSelectionDiv.append(
  fromDatetimeLabel, toDatetimeLabel,
  fromSlotLabel, toSlotLabel
)

const summaryDiv = document.createElement('div')
// TODO: add total duties assigned, completed, failed
// TODO: also breakdown by type of duty
// TODO: also add total rewards and penalties?

const detailsDiv = document.createElement('div')
// TODO: add square per subperiod coloured according to duty performance, laid out calendar-like
// TODO: add summary per subperiod as tooltip/title per square
// TODO: add selector for subperiod size (usually 1 day)

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
  perfHeading,
  slotsHeading,
  slotSelectionDiv,
  summaryHeading,
  summaryDiv,
  detailsHeading,
  detailsDiv
)
