const socket = io()
const frag = document.createDocumentFragment()
const body = document.querySelector('body')

const idReplacements = new Map([['%', 'P'], ['(', 'L'], [')', 'R']])
const toId = (s) => s.toLowerCase().replaceAll(
  /\s|%|(|)/g, (x) => idReplacements.get(x) ?? '-')

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
slotsHeading.innerText = 'Time Range (UTC)'
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
      bUp.value = '⬇️'
      bDown.value = '⬆️'
      bUp.title = 'sort column ascending'
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
slotSelectionDiv.id = 'slotSelection'
const fromDateLabel = document.createElement('label')
const fromDate = document.createElement('input')
const fromTimeLabel = document.createElement('label')
const fromTime = document.createElement('input')
const toDateLabel = document.createElement('label')
const toDate = document.createElement('input')
const toTimeLabel = document.createElement('label')
const toTime = document.createElement('input')
const fromSlotLabel = document.createElement('label')
const fromSlot = document.createElement('input')
const toSlotLabel = document.createElement('label')
const toSlot = document.createElement('input')
;[fromSlotLabel, toSlotLabel].forEach(e => e.classList.add('slotLabel'))

const slotRangeLimitsDiv = document.createElement('div')
slotRangeLimitsDiv.id = 'slotRangeLimits'
const limitFromDateLabel = document.createElement('label')
const limitFromDate = document.createElement('input')
const limitFromTimeLabel = document.createElement('label')
const limitFromTime = document.createElement('input')
const limitToDateLabel = document.createElement('label')
const limitToDate = document.createElement('input')
const limitToTimeLabel = document.createElement('label')
const limitToTime = document.createElement('input')
const limitFromSlotLabel = document.createElement('label')
const limitFromSlot = document.createElement('input')
const limitToSlotLabel = document.createElement('label')
const limitToSlot = document.createElement('input')
const limitFromSlotButton = document.createElement('input')
const limitToSlotButton = document.createElement('input')
;[limitFromSlotButton, limitToSlotButton].forEach(e => {
  e.type = 'button'
  e.value = 'Use'
  e.addEventListener('click', () => {
    const thisInput = e.parentElement.querySelector('input[type="number"]')
    const slotInput = slotSelectionDiv.querySelector(
      `input[type="number"][data-dir="${thisInput.dataset.dir}"]`
    )
    slotInput.value = thisInput.value
    slotInput.dispatchEvent(new Event('change'))
  }, {passive: true})
})

const slotRangeLimits = {min: 0, max: Infinity}
socket.emit('slotRangeLimits', [])

const limitFromDateTime = document.createElement('div')
const limitToDateTime = document.createElement('div')
limitFromDateTime.append(limitFromDateLabel, limitFromTimeLabel)
limitToDateTime.append(limitToDateLabel, limitToTimeLabel)
const limitFromSlotDiv = document.createElement('div')
const limitToSlotDiv = document.createElement('div')
limitFromSlotDiv.append(limitFromSlotLabel, limitFromSlotButton)
limitToSlotDiv.append(limitToSlotLabel, limitToSlotButton)
slotRangeLimitsDiv.append(
  limitFromDateTime, limitToDateTime,
  limitFromSlotDiv, limitToSlotDiv
)

;[fromDate, toDate, limitFromDate, limitToDate].forEach(e => e.type = 'date')
;[fromTime, toTime, limitFromTime, limitToTime].forEach(e => {
  e.type = 'time'
  e.step = 1
})
;[fromSlot, toSlot, limitFromSlot, limitToSlot].forEach(e => e.type = 'number')
;[fromSlot, toSlot].forEach(e => e.dataset.prevValue = e.value)
;[fromDate, fromTime, fromSlot, limitFromDate, limitFromTime, limitFromSlot].forEach(e => e.dataset.dir = 'from')
;[toDate, toTime, toSlot, limitToDate, limitToTime, limitToSlot].forEach(e => e.dataset.dir = 'to')
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
limitFromSlotLabel.append(
  document.createTextNode('Min (activation) slot: '),
  limitFromSlot
)
limitToSlotLabel.append(
  document.createTextNode('Max (finalized) slot: '),
  limitToSlot
)
limitFromDateLabel.append(
  document.createTextNode('Min date: '),
  limitFromDate
)
limitFromTimeLabel.append(limitFromTime)
limitToDateLabel.append(
  document.createTextNode('Max date: '),
  limitToDate
)
limitToTimeLabel.append(limitToTime)

const thisUrl = new URL(window.location)

const minipoolsInTable = () => Array.from(
  minipoolsList.querySelectorAll('td.minipool > a')
).flatMap(a => isIncluded(a) ? [a.href.slice(-42)] : [])

const validatorIndicesInTable = () => Array.from(
  minipoolsList.querySelectorAll('td.validator > a')
).flatMap(a => isIncluded(a) ? [a.innerText] : [])

const updatePerformanceDetails = () => {
  const fromValue = parseInt(fromSlot.value)
  const toValue = parseInt(toSlot.value)
  const minipools = minipoolsInTable()
  if (0 <= fromValue && fromValue <= toValue && minipools.length) {
    console.log(`Asking for perfDetails ${fromValue} - ${toValue}`)
    socket.volatile.emit('perfDetails', fromValue, toValue, minipools)
  }
  else {
    console.warn(`Rejected perfDetails ${fromValue} - ${toValue}`)
  }
}

const setTimeFromSlot = ({slot, dateInput, timeInput}) =>
  new Promise(resolve =>
    socket.emit('slotToTime', slot,
      ({date, time}) => {
        dateInput.value = date
        timeInput.value = time
        resolve()
      }
    )
  )

let callAfterValidatorsForSlots

async function updateSlotRange() {

  if (typeof callAfterValidatorsForSlots == 'function') {
    console.log(`Skipping updateSlotRange for callAfterValidatorsForSlots`)
    return
  }

  const fromOld = fromSlot.dataset.prevValue
  const toOld = toSlot.dataset.prevValue
  let [fromNew, toNew] = [fromSlot.value, toSlot.value].map(x => parseInt(x))

  if (isNaN(fromNew) || fromNew < slotRangeLimits.min)
    fromNew = slotRangeLimits.min

  if (isNaN(toNew) || slotRangeLimits.max < toNew)
    toNew = slotRangeLimits.max

  if (!(fromNew <= toNew))
    [toNew, fromNew] = [fromNew, toNew]

  await Promise.all(
    [{slot: fromNew, dateInput: fromDate, timeInput: fromTime},
     {slot: toNew, dateInput: toDate, timeInput: toTime}].map(
       setTimeFromSlot
     )
  )

  if (!(0 <= fromNew && 0 <= toNew)) {
    console.error(`Bad slot range ${fromNew} - ${toNew}, from ${fromOld} - ${toOld}`)
    return
  }

  fromSlot.min = slotRangeLimits.min
  fromSlot.max = toNew
  toSlot.min = fromNew
  toSlot.max = slotRangeLimits.max

  const rangeChanged = fromNew != fromOld || toNew != toOld

  if (rangeChanged || slotRangeLimits.validatorsChanged) {
    if (rangeChanged) {
      console.log(`Updating ${fromOld} - ${toOld} to ${fromNew} - ${toNew}`)
      fromSlot.value = fromNew
      toSlot.value = toNew
      fromSlot.dataset.prevValue = fromSlot.value
      toSlot.dataset.prevValue = toSlot.value
      thisUrl.searchParams.set('fromSlot', fromNew)
      thisUrl.searchParams.set('toSlot', toNew)
    }
    if (slotRangeLimits.validatorsChanged) {
      thisUrl.searchParams.delete('v')
      validatorIndicesInTable().forEach(i =>
        thisUrl.searchParams.append('v', i)
      )
    }
    window.history.pushState(null, '', thisUrl)
    updatePerformanceDetails()
    delete slotRangeLimits.validatorsChanged
  }
  else {
    console.log(`Unchanged (ignored): ${fromOld} - ${toOld} to ${fromNew} - ${toNew}`)
  }
}

const timeSelectionHandler = (e) => {
  const dir = e.target.dataset.dir
  const type = e.target.type
  const otherType = type === 'time' ? 'date' : 'time'
  const value = e.target.value
  const other = slotSelectionDiv.querySelector(`input[type="${otherType}"][data-dir="${dir}"]`)
  const datestring = type === 'time' ? `${other}T${value}` : `${value}T${other}`
  const time = (new Date(datestring)).getTime() / 1000
  const slotInput = slotSelectionDiv.querySelector(`input[type="number"][data-dir="${dir}"]`)
  socket.emit('timeToSlot', time, (slot) => {
    slotInput.value = slot
    updateSlotRange()
  })
}

;[fromDate, toDate, fromTime, toTime].forEach(e =>
  e.addEventListener('change', timeSelectionHandler, {passive: true})
)

;[fromSlot, toSlot].forEach(e =>
  e.addEventListener('change', updateSlotRange, {passive: true})
)

const rangeButtons = document.createElement('div')
rangeButtons.id = 'fullRangeButtons'
rangeButtons.classList.add('rangeButtons')

async function calculateSlotRange(period) {
  fromSlot.dataset.prevValue = fromSlot.value
  toSlot.dataset.prevValue = toSlot.value
  const date = new Date()
  fromSlot.value = slotRangeLimits.min
  toSlot.value = slotRangeLimits.max
  if (period === 'year') {
    date.setMonth(0, 1)
    date.setHours(0, 0, 0, 0)
  }
  else if (period === 'month') {
    date.setDate(1)
    date.setHours(0, 0, 0, 0)
  }
  else if (period === 'week') {
    date.setDate(date.getDate() - date.getDay())
    date.setHours(0, 0, 0, 0)
  }
  else if (period === 'today') {
    date.setHours(0, 0, 0, 0)
  }
  else if (period === 'hour') {
    date.setMinutes(0, 0, 0)
  }
  if (period !== 'time') {
    const time = date.getTime() / 1000
    await new Promise(resolve => {
      socket.emit('timeToSlot', time, (slot) => {
        fromSlot.value = slot
        resolve()
      })
    })
  }
  await updateSlotRange()
}

const makeButton = (v) => {
  const b = document.createElement('input')
  b.type = 'button'
  b.value = v
  b.addEventListener('click', () =>
    calculateSlotRange(v.split(/\s/).at(-1).toLowerCase())
  )
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
    addButton.title = `Increase range by one ${name}`
    subButton.title = `Decrease range by one ${name}`
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
  if (h == summaryHeadings[2]) return d.duties
    ? Math.round(100 * 100 * ((d.duties - d.missed) / d.duties)) / 100
    : 100
  if (h == summaryHeadings[3]) return formatGwei(d.reward)
  throw new Error(`Unknown heading ${h}`)
}
const allSummaryTable = document.createElement('table')
allSummaryTable.id = 'allSummaryTable'
allSummaryTable.classList.add('hidden')
const dutyHeadings = ['Attestations', 'Proposals', 'Syncs']
allSummaryTable.appendChild(document.createElement('tr'))
  .append(...summaryHeadings.map(h => {
    const th = document.createElement('th')
    th.innerText = h
    th.id = `th-${toId(h)}`
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

const summaryTable = document.createElement('table')
summaryTable.id = 'summaryTable'
summaryTable.classList.add('hidden')
summaryTable.appendChild(document.createElement('tr'))
  .append(...dutyHeadings.map(h => {
    const th = document.createElement('th')
    th.innerText = h
    th.setAttribute('colspan', '4')
    th.id = `th-${toId(h)}`
    return th
  }))
const summaryDutyHeadings = Array.from(
  summaryTable.lastElementChild.children
).flatMap(h =>
    summaryHeadings.map(h2 => {
      const th = document.createElement('th')
      th.innerText = h2
      th.headers = h.id
      th.id = `th-${h.id.slice(3)}-${toId(h2)}`
      return th
    })
)
summaryTable.appendChild(document.createElement('tr')).append(...summaryDutyHeadings)
Array.from(summaryTable.children).forEach(
  r => r.classList.add('head')
)
const summaryDutyEntries = Array.from(
  summaryTable.lastElementChild.children
).map(h => {
  const td = document.createElement('td')
  td.headers = h.id
  td.id = `td-${h.id.slice(3)}`
  return td
})
summaryTable.appendChild(document.createElement('tr')).append(...summaryDutyEntries)

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

socket.on('perfDetails', data => {
  // <data> = { <year>: {<month>: {<day>: {attestations: <dutyData>, proposals: <dutyData>, syncs: <dutyData>, slots: {min: <num>, max: <num>}}, ...}, ...}, ...}
  // <dutyData> = { duties: <num>, missed: <num>, reward: <string(bigint)>, slots: <Array[num]> }
  // console.log(`Received perfDetails: ${JSON.stringify(data)}`)
  const totals = {...emptyDay()}
  const addTotals = (day) => Object.entries(day).forEach(([k, v]) => k != 'slots' && addTotal(totals[k], v))
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
        const {totalDuties, totalMissed} = Object.values(dayObj).filter(d => 'duties' in d).reduce(
          ({totalDuties, totalMissed}, {duties, missed}) =>
          ({totalDuties: totalDuties + duties, totalMissed: totalMissed + missed}),
          {totalDuties: 0, totalMissed: 0}
        )
        const performance = (totalDuties - totalMissed) / totalDuties
        const performanceDecile = totalDuties ?
          (totalMissed ? Math.round(performance * 10) * 10 : 'all')
          : 'nil'
        dayDiv.classList.add(`perf${performanceDecile}`)
        const dayObjKeys = Object.keys(dayObj).slice(0, -1)
        dayObjKeys.forEach(k => dayObj[k].reward = BigInt(dayObj[k].reward))
        const proposalSlots = (key, slots) => key === 'proposals' ? ` (${slots.join(',')})` : ''
        const dutyLine = (key, {duties, missed, reward, slots}) =>
          `${duties - missed}/${duties}${proposalSlots(key, slots)}: ${formatGwei(reward)} gwei`
        const dutyTitle = (key) => (
          (dayObj[key].duties || dayObj[key].reward) &&
          dutyLine(key, dayObj[key])
        )
        const titleLines = [`${dayObj.slots.min}–${dayObj.slots.max}`].concat(
          dayObjKeys.flatMap(k => {
            const t = dutyTitle(k)
            return t ? [`${k[0].toUpperCase()}: ${t}`] : []
          })
        )
        dayDiv.title = titleLines.join('\n')
        if (dayObj.proposals.duties)
          dayDiv.classList.add('proposer')
        addTotals(dayObj)
      }
    }
  }
  detailsDiv.replaceChildren(frag)
  const allSummaryTotals = {...emptyDutyData}
  Object.values(totals).forEach(d => addTotal(allSummaryTotals, d))
  for (const h of summaryHeadings) {
    const td = document.getElementById(`td-${toId(h)}`)
    td.innerText = summaryFromDuty(h, allSummaryTotals)
  }
  for (const [dh, duty] of Object.entries(totals)) {
    for (const h of summaryHeadings) {
      const td = document.getElementById(`td-${toId(dh)}-${toId(h)}`)
      td.innerText = summaryFromDuty(h, duty)
    }
  }
  if (allSummaryTotals.duties) {
    allSummaryTable.classList.remove('hidden')
    summaryTable.classList.remove('hidden')
  }
  else {
    allSummaryTable.classList.add('hidden')
    summaryTable.classList.add('hidden')
  }
})

socket.on('minipools', minipools => {
  console.log(`Received ${minipools.length} minipools`)
  for (const {minipoolAddress, minipoolEnsName,
              nodeAddress, nodeEnsName,
              withdrawalAddress, withdrawalEnsName,
              validatorIndex} of minipools) {
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
    sel.checked = true
    sel.addEventListener('change', updateIncludeAllChecked, {passive: true})
    sel.addEventListener('change',
      () => {
        slotRangeLimits.validatorsChanged = true
        socket.volatile.emit('slotRangeLimits', validatorIndicesInTable())
      },
      {passive: true}
    )
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
    slotRangeLimits.validatorsChanged = true
    socket.volatile.emit('slotRangeLimits',
      minipools.map(({validatorIndex}) => validatorIndex)
    )
  }
  else minipoolsList.classList.add('hidden')
  if (typeof callAfterValidatorsForSlots == 'function')
    callAfterValidatorsForSlots()
})

socket.on('slotRangeLimits', ({min, max}) => {
  slotRangeLimits.min = min
  slotRangeLimits.max = max
  limitFromSlot.value = min
  limitToSlot.value = max
  Promise.all(
    [{slot: min, dateInput: limitFromDate, timeInput: limitFromTime},
     {slot: max, dateInput: limitToDate, timeInput: limitToTime}].map(
       setTimeFromSlot
     )
  ).then(updateSlotRange)
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
  slotRangeLimitsDiv,
  slotSelectionDiv,
  summaryHeading,
  allSummaryTable,
  summaryTable,
  detailsHeading,
  detailsDiv,
  footerDiv
)

slotRangeLimitsDiv.querySelectorAll('input').forEach(
  x => x.setAttribute('readonly', '')
)

async function setParamsFromUrl() {
  thisUrl.href = window.location

  console.log(`Setting params from ${thisUrl.searchParams}`)
  const slotsToSet = [fromSlot, toSlot].map(input => (
    {input, slot: thisUrl.searchParams.get(`${input.dataset.dir}Slot`)}
  ))

  let promise

  const urlValidators = thisUrl.searchParams.getAll('v')
  if (urlValidators.length) {
    promise = new Promise(resolve => callAfterValidatorsForSlots = resolve)
    entityEntryBox.value = urlValidators.join('\n')
    entityEntryBox.dispatchEvent(new Event('change'))
  }

  await promise.then(() => callAfterValidatorsForSlots = undefined)

  promise = false
  slotsToSet.forEach(({input, slot}) => {
    if ((slot || slot === 0) && input.value != slot) {
      input.dataset.prevValue = input.value
      input.value = slot
      promise = true
    }
  })
  if (promise) await updateSlotRange()
}

window.addEventListener('popstate', setParamsFromUrl, {passive: true})

setParamsFromUrl()

// TODO: fix slot selection from URL on initial load
// TODO: handle URL length limits - just drop some validators? or make a more compressed string?
// TODO: add loading (and out-of-date) indication for results/details
// TODO: improve performance for loading results (caching? do more on client? parallelise fetching per day/month?, caching client-side)
// TODO: add attestation accuracy and reward info
// TODO: disable add/sub buttons when they won't work?
// TODO: add buttons to zero out components of the time, e.g. go to start of day, go to start of week, go to start of month, etc.?
// TODO: server sends "update minimum/maximum slot" messages whenever finalized increases?
// TODO: add NO portion of rewards separately from validator rewards? (need to track commission and borrow)
// TODO: add copy for whole table?
// TODO: make certain css colors (and maybe other styles) editable?
// TODO: make weekday start configurable (Sun vs Mon)?
// TODO: add selector for subperiod sizes (instead of year/month/day)?
// TODO: look into execution layer rewards too? probably ask for more money to implement that
// TODO: add free-form text selectors for times too?
// const timezoneLabel = document.createElement('label') TODO: add timezone selection?
