[...document.querySelectorAll('tbody tr')].map(x => {
  const row = [...x.querySelectorAll('td')]
    return {
    obwod: row[0].innerText,
    glosy: parseInt(row[3].innerText)
  }
})