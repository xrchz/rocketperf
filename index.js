const socket = io()
const frag = document.createDocumentFragment()
const body = document.querySelector('body')

const toId = (s) => s.replace(/\s/,'-').replace(/%/,'percent')

// TODO: store parts of the state (e.g: minipools, slot range) in URL query

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
  () => socket.volatile.emit('entities', entityEntryBox.value),
  {passive: true}
)

const entityFailures = document.createElement('ul')
entityFailures.id = 'entityFailures'

const isIncluded = (a) =>
  a.parentElement.parentElement
    .querySelector('input[type="checkbox"]').checked

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

const stringCollator = new Intl.Collator()
const numericCollator = new Intl.Collator(undefined, {numeric: true})
const sortUp = collator => (a, b) => collator.compare(a.dataset.sortKey, b.dataset.sortKey)
const sortDown = collator => (a, b) => collator.compare(b.dataset.sortKey, a.dataset.sortKey)
function sortByColumn(compareFn, headerId) {
  const rows = []
  Array.from(
    minipoolsList.querySelectorAll('tr')
  ).forEach(row =>
    row.classList.contains('head')
    ? frag.appendChild(row)
    : rows.push(row)
  )
  minipoolsList.replaceChildren(frag)
  rows.forEach(row => {
    const item = row.querySelector(`td[headers~="${headerId}"] > :is(a, input)`)
    row.dataset.sortKey = item.tagName == 'A' ? item.innerText : item.checked.toString()
  })
  rows.sort(compareFn)
  rows.forEach(row => delete row.dataset.sortKey)
  minipoolsList.append(...rows)
}

minipoolsList.appendChild(document.createElement('tr'))
  .append(
    ...headings.map((h, i) => {
      const th = document.createElement('th')
      th.classList.add('columnTools')
      const headerId = `th-${h.toLowerCase()}`
      const bUp = document.createElement('input')
      const bDown = document.createElement('input')
      const bCopy = document.createElement('input')
      bUp.value = '⬆️'
      bUp.title = 'sort column ascending'
      bDown.value = '⬇️'
      bDown.title = 'sort column descending'
      bCopy.value = '📋'
      bCopy.title = 'copy unique included column items'
      bCopy.addEventListener('click', () => {
        const columnText = Array.from(
          new Set(
            Array.from(
              minipoolsList.querySelectorAll(`td[headers~="${headerId}"] > a`)
            ).flatMap(a => isIncluded(a) ? [a.innerText] : [])
          ).values()
        ).join('\n')
        if (columnText.length)
          navigator.clipboard.writeText(columnText)
      }, {passive: true})
      const collator = h == 'Validator' ? numericCollator : stringCollator
      bUp.addEventListener('click', () => sortByColumn(sortUp(collator), headerId), {passive: true})
      bDown.addEventListener('click', () => sortByColumn(sortDown(collator), headerId), {passive: true})
      const buttons = [bUp, bDown, bCopy]
      buttons.forEach(b => b.type = 'button')
      if (h == 'Include') {
        const ch = document.createElement('input')
        ch.id = 'include-all-checked'
        ch.type = 'checkbox'
        ch.addEventListener('change', () => {
          Array.from(
            minipoolsList.querySelectorAll('input[type="checkbox"]')
          ).forEach(e => e.checked = ch.checked)
        })
        th.append(...buttons.toSpliced(-1, 1, ch))
      }
      else
        th.append(...buttons)
      return th
    })
  )
Array.from(minipoolsList.children).forEach(
  r => r.classList.add('head')
)

function updateIncludeAllChecked() {
  const boxes = Array.from(
    minipoolsList.querySelectorAll('input[type="checkbox"]')
  )
  const checked = boxes.filter(e => e.checked)
  const allChecked = document.getElementById('include-all-checked')
  const numBoxes = boxes.length - 1
  const numChecked = checked.length - allChecked.checked
  allChecked.indeterminate = false
  if (numChecked == 0) allChecked.checked = false
  else if (numChecked == numBoxes) allChecked.checked = true
  else allChecked.indeterminate = true
}

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
  socket.volatile.emit('setSlot',
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
).flatMap(a => isIncluded(a) ? [a.href.slice(-42)] : [])

const updatePerformanceDetails = () => {
  const fromValue = parseInt(fromSlot.value)
  const toValue = parseInt(toSlot.value)
  const minipools = minipoolsInTable()
  if (0 <= fromValue && fromValue <= toValue && minipools.length) {
    socket.volatile.emit('perfDetails', fromValue, toValue, minipools)
  }
}

function ensureValidRange() {
  const fromOld = fromSlot.value, toOld = toSlot.value
  if (!(parseInt(fromSlot.value) <= parseInt(toSlot.value))) {
    if (!fromSlot.value) { toSlot.value = fromSlot.value }
    toSlot.value = fromSlot.value
  }
  let changed = false
  if (fromOld != fromSlot.value && !isNaN(parseInt(fromSlot.value))) {
    fromSlot.dispatchEvent(new Event('change'))
    changed = true
  }
  if (toOld != toSlot.value && !isNaN(parseInt(toSlot.value))) {
    toSlot.dispatchEvent(new Event('change'))
    changed = true
  }
  return changed
}

socket.on('setSlot', (key, value) => {
  const elt = slotSelectors.get(key)
  const oldValue = elt.value
  elt.value = value
  if (key.endsWith('Slot')) {
    if (oldValue !== value && !ensureValidRange())
      updatePerformanceDetails()
  }
})

const rangeButtons = document.createElement('div')
rangeButtons.id = 'fullRangeButtons'
rangeButtons.classList.add('rangeButtons')

const makeButton = (v) => {
  const b = document.createElement('input')
  b.type = 'button'
  b.value = v
  b.addEventListener('click', () =>
    socket.volatile.emit('setSlotRange', v.split(/\s/).at(-1).toLowerCase()))
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

// TODO: add option to select minimum activated slot for selected validators /
// make the minimum fromSlot be this

const fromButtons = document.createElement('div')
const toButtons = document.createElement('div')
fromButtons.classList.add('dirRangeButtons')
fromButtons.classList.add('from')
toButtons.classList.add('dirRangeButtons')
toButtons.classList.add('to')
const secondsPerSlot = 12
const minutesPerHour = 60
const secondsPerMinute = 60
const secondsPerHour = minutesPerHour * secondsPerMinute
const slotsPerHour = secondsPerHour / secondsPerSlot
const hoursPerDay = 24
const slotsPerDay = slotsPerHour * hoursPerDay
const daysPerWeek = 7
const slotsPerWeek = slotsPerDay * daysPerWeek
const daysPerMonth = 30.4375
const slotsPerMonth = Math.round(slotsPerDay * daysPerMonth)
const daysPerYear = 365.25
const slotsPerYear = Math.round(slotsPerDay * daysPerYear)
const timeIncrements = [
  {name: 'hour',  slots: slotsPerHour},
  {name: 'day',   slots: slotsPerDay},
  {name: 'week',  slots: slotsPerWeek},
  {name: 'month', slots: slotsPerMonth},
  {name: 'year',  slots: slotsPerYear}
].reverse()
const add = (x, y) => x + y
const sub = (x, y) => x - y
for (const {name, slots} of timeIncrements) {
  function makeAddSub(target) {
    const addButton = document.createElement('input')
    const subButton = document.createElement('input')
    addButton.type = 'button'
    subButton.type = 'button'
    addButton.value = `+${name[0]}`
    subButton.value = `-${name[0]}`
    function makeHandler(op) {
      return () => {
        const currentValue = parseInt(target.value)
        if (typeof currentValue == 'number') {
          target.value = op(currentValue, slots)
          target.dispatchEvent(new Event('change'))
        }
      }
    }
    addButton.addEventListener('click', makeHandler(add), {passive: true})
    subButton.addEventListener('click', makeHandler(sub), {passive: true})
    return [addButton, subButton]
  }
  fromButtons.append(...makeAddSub(fromSlot))
  toButtons.append(...makeAddSub(toSlot))
}
// TODO: disable buttons when they won't work?

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

const summaryHeadings = ['Assigned', 'Missed', 'Success Rate (%)', 'Net Reward (gwei)']
const summaryFromDuty = (h, d) => {
  if (h == summaryHeadings[0]) return d.duties
  if (h == summaryHeadings[1]) return d.missed
  if (h == summaryHeadings[2]) return Math.round(100 * 100 * ((d.duties - d.missed) / d.duties)) / 100
  if (h == summaryHeadings[3]) return formatGwei(d.reward)
  throw new Error(`Unknown heading ${h}`)
}
const allSummaryTable = document.createElement('table')
allSummaryTable.id = 'allSummaryTable'
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
allSummaryTable.firstElementChild.classList.add('head')
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
Array.from(summaryTable.children).forEach(
  r => r.classList.add('head')
)

// TODO: fill tables
// TODO: add attestation accuracy and reward info

const detailsDiv = document.createElement('div')
detailsDiv.id = 'details'

const compareNumbers = (a,b) => a - b
const monthNames = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']

const emptyDutyData = { duties: 0, missed: 0, reward: 0n }
const emptyDay = () => ({
  attestations: {...emptyDutyData},
  proposals: {...emptyDutyData},
  syncs: {...emptyDutyData}
})
const addTotal = (t, d) => Object.keys(t).forEach(k => t[k] += d[k])

// TODO: make weekday start configurable (Sun vs Mon)

socket.on('perfDetails', data => {
  // <data> = { <year>: {<month>: {<day>: {attestations: <dutyData>, proposals: <dutyData>, syncs: <dutyData>}, ...}, ...}, ...}
  // <dutyData> = { duties: <num>, missed: <num>, reward: <string(bigint)> }
  // console.log(`Received perfDetails: ${JSON.stringify(data)}`)
  const totals = {...emptyDay()}
  const addTotals = (day) => Object.entries(day).forEach(([k, v]) => addTotal(totals[k], v))
  for (const year of Object.keys(data).map(k => parseInt(k)).toSorted(compareNumbers)) {
    const yearContainer = frag.appendChild(document.createElement('div'))
    yearContainer.classList.add('yearContainer')
    yearContainer.appendChild(document.createElement('span')).innerText = year
    const yearDiv = yearContainer.appendChild(document.createElement('div'))
    yearDiv.classList.add('year')
    const yearObj = data[year]
    for (const month of Object.keys(yearObj).map(k => parseInt(k)).toSorted(compareNumbers)) {
      const monthContainer = yearDiv.appendChild(document.createElement('div'))
      monthContainer.classList.add('monthContainer')
      monthContainer.appendChild(document.createElement('span')).innerText = monthNames[month].slice(0, 3)
      const monthDiv = monthContainer.appendChild(document.createElement('div'))
      monthDiv.classList.add('month')
      const monthObj = yearObj[month]
      const days = Object.keys(monthObj).map(k => parseInt(k)).toSorted(compareNumbers)
      const spacerDays = ((new Date(`${year}-${month + 1}-${days[0]}`)).getUTCDay() + 6) % 7
      for (const spacerDay of Array(spacerDays).fill()) {
        const dayDiv = monthDiv.appendChild(document.createElement('div'))
        dayDiv.classList.add('day')
        dayDiv.classList.add('spacer')
      }
      for (const day of days) {
        const dayObj = monthObj[day]
        const dayDiv = monthDiv.appendChild(document.createElement('div'))
        dayDiv.classList.add('day')
        dayDiv.appendChild(document.createElement('span')).innerText = day
        const {totalDuties, totalMissed} = Object.values(dayObj).reduce(
          ({totalDuties, totalMissed}, {duties, missed}) =>
          ({totalDuties: totalDuties + duties, totalMissed: totalMissed + missed}),
          {totalDuties: 0, totalMissed: 0}
        )
        const performance = (totalDuties - totalMissed) / totalDuties
        const performanceDecile = totalDuties ?
          (totalMissed ? Math.round(performance * 10) * 10 : 'all')
          : 'nil'
        dayDiv.classList.add(`perf${performanceDecile}`)
        const dayObjKeys = Object.keys(dayObj)
        dayObjKeys.forEach(k => dayObj[k].reward = BigInt(dayObj[k].reward))
        const dutyTitle = (key) => (
          (dayObj[key].duties || dayObj[key].reward) &&
          `${dayObj[key].duties - dayObj[key].missed}/${dayObj[key].duties}: ${formatGwei(dayObj[key].reward)} gwei`
        )
        const titleLines = dayObjKeys.flatMap(k => {
            const t = dutyTitle(k)
            return t ? [`${k[0].toUpperCase()}: ${t}`] : []
          })
        dayDiv.title = titleLines.join('\n')
        addTotals(dayObj)
      }
    }
  }
  detailsDiv.replaceChildren(frag)
  const allSummaryTotals = {...emptyDutyData}
  Object.values(totals).forEach(d => addTotal(allSummaryTotals, d))
  for (const h of summaryHeadings) {
    const td = document.getElementById(`td-${toId(h.toLowerCase())}`)
    td.innerText = summaryFromDuty(h, allSummaryTotals)
  }
  if (allSummaryTotals.duties)
    allSummaryTable.classList.remove('hidden')
  else
    allSummaryTable.classList.add('hidden')
  // TODO: add totals to summaryTable
})

// TODO: make select-all, select-none option for include work

// TODO: make the sorting options for the columns work

// TODO: add selector for subperiod sizes (instead of year/month/day)?

// TODO: add copy for whole table?

// TODO: add loading indication for details

socket.on('minipools', minipools => {
  console.log(`Received minipools`)
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
    sel.addEventListener('change', updateIncludeAllChecked, {passive: true})
    // TODO: updatePerformanceDetails when sel has changed after some delay to indicate changing has stopped?
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
  minipoolsList.replaceChildren(
    ...Array.from(minipoolsList.querySelectorAll('tr.head'))
  )
  minipoolsList.appendChild(frag)
  updateIncludeAllChecked()
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
footerDiv.id = 'footer'
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
