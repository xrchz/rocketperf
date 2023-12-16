const socket = io()
const frag = document.createDocumentFragment()
const body = document.querySelector('body')

const toId = (s) => s.replace(/\s/,'-')

function formatUnits(wei, decimals) {
  const {val, neg} = wei < 0n ? {val: wei * -1n, neg: '-'} : {val: wei, neg: ''}
  const padded = val.toString().padStart(decimals, '0')
  const index = padded.length - decimals
  const dotted = `${padded.slice(0, index)}.${padded.slice(index)}`
  const trimLeft = dotted.replace(/^0*\./, '0.')
  const trimRight = trimLeft.replace(/0*$/, '')
  const trimDot = trimRight.replace(/\.$/, '')
  return `${neg}${trimDot}`
}
const formatEther = wei => formatUnits(wei, 18)
const formatGwei = wei => formatUnits(wei, 9)

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
slotsHeading.innerText = 'Time Range (UTC only for now) (till finalized slot)'
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

const entityFailures = document.createElement('ul')
entityFailures.id = 'entityFailures'

const minipoolsList = document.createElement('table')
minipoolsList.classList.add('hidden')
const headings = ['Minipool', 'Node', 'Withdrawal', 'Validator', 'Include']
minipoolsList.appendChild(document.createElement('tr'))
  .append(
    ...headings.map(h => {
      const th = document.createElement('th')
      th.id = `th-${h.toLowerCase()}`
      th.innerText = h
      return th
    })
  )

const slotSelectionDiv = document.createElement('div')
const fromDateLabel = document.createElement('label')
const fromDate = document.createElement('input')
const fromTimeLabel = document.createElement('label')
const fromTime = document.createElement('input')
const toDateLabel = document.createElement('label')
const toDate = document.createElement('input')
const toTimeLabel = document.createElement('label')
const toTime = document.createElement('input')
// const timezoneLabel = document.createElement('label') TODO: add this later?
const fromSlotLabel = document.createElement('label')
const fromSlot = document.createElement('input')
const toSlotLabel = document.createElement('label')
const toSlot = document.createElement('input')
;[fromSlotLabel, toSlotLabel].forEach(e => e.classList.add('slotLabel'))
const slotSelectors = new Map()
slotSelectors.set('fromDate', fromDate)
slotSelectors.set('fromTime', fromTime)
slotSelectors.set('toDate', toDate)
slotSelectors.set('toTime', toTime)
slotSelectors.set('fromSlot', fromSlot)
slotSelectors.set('toSlot', toSlot)

slotSelectionDiv.id = 'slotSelection'

// TODO: add free-form text selectors for times too?

;[fromDate, toDate].forEach(e => e.type = 'date')
;[fromTime, toTime].forEach(e => {
  e.type = 'time'
  e.step = 1
})
;[fromSlot, toSlot].forEach(e => {
  e.type = 'number'
  e.min = 0
})
;[fromDate, fromTime, fromSlot].forEach(e => e.dataset.dir = 'from')
;[toDate, toTime, toSlot].forEach(e => e.dataset.dir = 'to')
fromSlotLabel.append(
  document.createTextNode('From slot: '),
  fromSlot
)
toSlotLabel.append(
  document.createTextNode('To slot: '),
  toSlot
)
fromDateLabel.append(
  document.createTextNode('From date: '),
  fromDate
)
fromTimeLabel.append(fromTime)
toDateLabel.append(
  document.createTextNode('To date: '),
  toDate
)
toTimeLabel.append(toTime)

const slotSelectionHandler = (e) => {
  const dir = e.target.dataset.dir
  const type = e.target.type
  const otherType = type === 'time' ? 'Date' : type === 'date' ? 'Time' : null
  socket.emit('setSlot',
    {dir, type, value: e.target.value,
     other: otherType && slotSelectors.get(`${dir}${otherType}`).value
    }
  )
}

slotSelectors.forEach(e =>
  e.addEventListener('change', slotSelectionHandler, {passive: true})
)

const minipoolsInTable = () => Array.from(
  minipoolsList.querySelectorAll('td.minipool > a')
).flatMap(a =>
  a.parentElement.parentElement
    .querySelector('input[type="checkbox"]').checked ?
  [a.href.slice(-42)] : []
)

const updatePerformanceDetails = () => {
  const fromValue = parseInt(fromSlot.value)
  const toValue = parseInt(toSlot.value)
  if (0 <= fromValue && fromValue <= toValue) {
    socket.volatile.emit('perfDetails',
      fromValue, toValue, minipoolsInTable()
    )
  }
}

function ensureValidRange() {
  const fromOld = fromSlot.value, toOld = toSlot.value
  if (!(parseInt(fromSlot.value) <= parseInt(toSlot.value))) {
    if (!fromSlot.value) { toSlot.value = fromSlot.value }
    toSlot.value = fromSlot.value
  }
  if (fromOld != fromSlot.value && !isNaN(parseInt(fromSlot.value))) fromSlot.dispatchEvent(new Event('change'))
  if (toOld != toSlot.value && !isNaN(parseInt(toSlot.value))) toSlot.dispatchEvent(new Event('change'))
}

socket.on('setSlot', (key, value) => {
  slotSelectors.get(key).value = value
  if (key.endsWith('Slot')) ensureValidRange()
  updatePerformanceDetails()
})

const rangeButtons = document.createElement('div')
rangeButtons.id = 'fullRangeButtons'
rangeButtons.classList.add('rangeButtons')

const makeButton = (v) => {
  const b = document.createElement('input')
  b.type = 'button'
  b.value = v
  b.addEventListener('click', () =>
    socket.emit('setSlotRange', v.split(/\s/).at(-1).toLowerCase()))
  return b
}

const allTimeButton = makeButton('All Time')
const lastYearButton = makeButton('This Year')
const lastMonthButton = makeButton('This Month')
const lastWeekButton = makeButton('This Week')
const lastDayButton = makeButton('Today')
const lastHourButton = makeButton('This Hour')

rangeButtons.append(
  allTimeButton,
  lastYearButton,
  lastMonthButton,
  lastWeekButton,
  lastDayButton,
  lastHourButton
)

const fromButtons = document.createElement('div')
const toButtons = document.createElement('div')
fromButtons.classList.add('dirRangeButtons')
toButtons.classList.add('dirRangeButtons')
// TODO: add actions to these buttons

const fromDateTime = document.createElement('div')
const toDateTime = document.createElement('div')
fromDateTime.append(fromDateLabel, fromTimeLabel)
toDateTime.append(toDateLabel, toTimeLabel)

slotSelectionDiv.append(
  rangeButtons,
  fromButtons, toButtons,
  fromDateTime, toDateTime,
  fromSlotLabel, toSlotLabel
)

const summaryHeadings = ['Assigned', 'Missed', 'Success Rate', 'Net Reward']
const allSummaryTable = document.createElement('table')
const summaryTable = document.createElement('table')
summaryTable.classList.add('hidden')
allSummaryTable.classList.add('hidden')
const dutyHeadings = ['Attestations', 'Proposals', 'Syncs']
allSummaryTable.appendChild(document.createElement('tr'))
  .append(...summaryHeadings.map(h => {
    const th = document.createElement('th')
    th.innerText = h
    th.id = `th-${toId(h.toLowerCase())}`
    return th
  }))
allSummaryTable.appendChild(document.createElement('tr'))
  .append(...Array.from(allSummaryTable.firstElementChild.children).map(h => {
    const td = document.createElement('td')
    td.headers = h.id
    td.id = `td-${h.id.slice(3)}`
    return td
  }))

summaryTable.appendChild(document.createElement('tr'))
  .append(...dutyHeadings.map(h => {
    const th = document.createElement('th')
    th.innerText = h
    th.setAttribute('colspan', '4')
    th.id = `th-${toId(h.toLowerCase())}`
    return th
  }))
summaryTable.appendChild(document.createElement('tr'))
  .append(...Array.from(summaryTable.firstElementChild.children).flatMap(h =>
    summaryHeadings.map(h2 => {
      const th = document.createElement('th')
      th.innerText = h2
      th.headers = h.id
      th.id = `th-${h.id.slice(3)}-${toId(h2.toLowerCase())}`
      return th
    })
  ))

// TODO: fill tables
// TODO: add attestation accuracy info

const detailsDiv = document.createElement('div')

const compareNumbers = (a,b) => a - b
const monthNames = ['January', 'February', 'March', 'April', 'March', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']

const emptyDutyData = { duties: 0, missed: 0, reward: 0n }
const emptyDay = {
  attestations: {...emptyDutyData},
  proposals: {...emptyDutyData},
  syncs: {...emptyDutyData}
}
const addTotal = (t, d) => Object.keys(t).forEach(k => t[k] += d[k])

socket.on('perfDetails', data => {
  // <data> = { <year>: {<month>: {<day>: {attestations: <dutyData>, proposals: <dutyData>, syncs: <dutyData>}, ...}, ...}, ...}
  // <dutyData> = { duties: <num>, missed: <num>, reward: <string(bigint)> }
  const totals = {...emptyDay}
  const addTotals = (day) => Object.entries(day).forEach(([k, v]) => addTotal(totals[k], v))
  for (const year of Object.keys(data).toSorted(compareNumbers)) {
    const yearDiv = frag.appendChild(document.createElement('div'))
    yearDiv.classList.add('year')
    yearDiv.appendChild(document.createElement('span')).innerText = year
    const yearObj = data[year]
    for (const month of Object.keys(yearObj).toSorted(compareNumbers)) {
      const monthDiv = yearDiv.appendChild(document.createElement('div'))
      monthDiv.classList.add('month')
      monthDiv.appendChild(document.createElement('span')).innerText = monthNames[month].slice(0, 3)
      const monthObj = yearObj[month]
      for (const day of Object.keys(monthObj).toSorted(compareNumbers)) {
        const dayObj = monthObj[day]
        const dayDiv = monthDiv.appendChild(document.createElement('div'))
        dayDiv.classList.add('day')
        const {totalDuties, totalMissed} = Object.values(dayObj).reduce(
          ({totalDuties, totalMissed}, {duties, missed}) =>
          ({totalDuties: totalDuties + duties, totalMissed: totalMissed + missed}))
        const performance = (totalDuties - totalMissed) / totalDuties
        const performanceDecile = Math.round(performance * 10)
        dayDiv.classList.add(`perf${performanceDecile}`)
        Object.keys(dayObj).forEach(k => dayObj[k].reward = BigInt(dayObj[k].reward))
        const dutyTitle = (key) =>
          `${dayObj[key].duties - dayObj[key].missed}/${dayObj[key].duties}: ${formatGwei(dayObj[key].reward)} gwei`
        dayDiv.title = `A: ${dutyTitle('attestations')}\nP: ${dutyTitle('proposals')}\nS: ${dutyTitle('syncs')}`
        addTotals(dayObj)
      }
    }
  }
  detailsDiv.replaceChildren(frag)
  // TODO: add totals to summary table
})

// TODO: add selector for subperiod sizes (instead of year/month/day)?

// TODO: add copy button for addresses in minipoolsList, and copy for whole columns, and whole table?

socket.on('minipools', minipools => {
  for (const {minipoolAddress, minipoolEnsName,
              nodeAddress, nodeEnsName,
              withdrawalAddress, withdrawalEnsName,
              validatorIndex, selected} of minipools) {
    const tr = frag.appendChild(document.createElement('tr'))
    const mpA = document.createElement('a')
    mpA.href = `https://rocketscan.io/minipool/${minipoolAddress}`
    mpA.innerText = minipoolEnsName || minipoolAddress
    const nodeA = document.createElement('a')
    nodeA.href = `https://rocketscan.io/node/${nodeAddress}`
    nodeA.innerText = nodeEnsName || nodeAddress
    const wA = document.createElement('a')
    wA.href = `https://rocketscan.io/address/${withdrawalAddress}`
    wA.innerText = withdrawalEnsName || withdrawalAddress
    const valA = document.createElement('a')
    valA.href = `https://beaconcha.in/validator/${validatorIndex}`
    valA.innerText = validatorIndex
    const sel = document.createElement('input')
    sel.type = 'checkbox'
    sel.checked = selected
    sel.addEventListener('change', updatePerformanceDetails, {passive: true})
    tr.append(
      ...[mpA, nodeA, wA, valA, sel].map((a, i) => {
        const td = document.createElement('td')
        td.appendChild(a)
        td.headers = `th-${headings[i].toLowerCase()}`
        return td
      })
    )
    mpA.parentElement.classList.add('minipool')
    if (!minipoolEnsName) mpA.parentElement.classList.add('address')
    nodeA.parentElement.classList.add('node')
    if (!nodeEnsName) nodeA.parentElement.classList.add('address')
    wA.parentElement.classList.add('withdrawal')
    if (!withdrawalEnsName) wA.parentElement.classList.add('address')
    valA.parentElement.classList.add('validator')
    sel.parentElement.classList.add('selected')
  }
  minipoolsList.replaceChildren(minipoolsList.firstElementChild)
  minipoolsList.appendChild(frag)
  if (minipools.length) {
    minipoolsList.classList.remove('hidden')
    updatePerformanceDetails()
  }
  else minipoolsList.classList.add('hidden')
})

socket.on('unknownEntities', entities => {
  console.log(`Got ${entities.length} unknownEntities`)
  entityFailures.replaceChildren(
    ...entities.map(s => {
      const li = document.createElement('li')
      li.innerText = s
      return li
    })
  )
})

const footerDiv = document.createElement('div')
const codeLink = document.createElement('a')
codeLink.href = 'https://github.com/xrchz/rocketperf'
codeLink.innerText = 'site code'
footerDiv.append(codeLink)

body.append(
  titleHeading,
  entryHeading,
  entityEntryBox,
  entityFailures,
  selectedHeading,
  minipoolsList,
  perfHeading,
  slotsHeading,
  slotSelectionDiv,
  summaryHeading,
  allSummaryTable,
  summaryTable,
  detailsHeading,
  detailsDiv,
  footerDiv
)
