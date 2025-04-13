const blessed = require("blessed");

// Create screen
const screen = blessed.screen({
  smartCSR: true,
  title: "Distributed Search Engine",
});

// Create search input
const searchInput = blessed.textbox({
  parent: screen,
  top: 0,
  left: 0,
  width: "100%",
  height: 3,
  border: { type: "line" },
  label: " Search Query ",
  inputOnFocus: true,
  keys: true,
  style: {
    border: { fg: "blue" },
    focus: { border: { fg: "green" } },
  },
});

const resultsList = blessed.list({
  parent: screen,
  top: 3,
  left: 0,
  width: "60%",
  height: "75%",
  border: { type: "line" },
  label: " Results ",
  tags: true, // Enable formatting tags in text
  keys: true, // Enable keyboard navigation
  vi: true, // Enable vi-like keybindings
  mouse: true, // Enable mouse interactions
  scrollable: true,
  scrollbar: {
    ch: " ",
    track: { bg: "gray" },
    style: { bg: "blue" },
  },
  style: {
    selected: { bg: "blue", fg: "white" },
    item: { fg: "white" },
  },
});

const detailsPanel = blessed.box({
  parent: screen,
  top: 3,
  left: "60%",
  width: "40%",
  height: "75%",
  border: { type: "line" },
  label: " Details ",
  tags: true,
  scrollable: true,
  scrollbar: {
    ch: " ",
    track: { bg: "gray" },
    style: { bg: "blue" },
  },
  content: "Select a result to see details",
});

// Create status bar
const statusBar = blessed.box({
  parent: screen,
  bottom: 0,
  left: 0,
  width: "100%",
  height: 3,
  border: { type: "line" },
  style: {
    border: { fg: "blue" },
  },
  content: "Ready",
});

// Render the screen
screen.render();

let searchResults = [];
let currentPage = 0;
let totalPages = 0;
const RESULTS_PER_PAGE = 10;
