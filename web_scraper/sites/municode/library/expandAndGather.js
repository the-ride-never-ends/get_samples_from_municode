/**
 * Expands and gathers hierarchical data from a webpage's DOM structure.
 *
 * This asynchronous function identifies root nodes based on anchor elements,
 * expands nested elements by clicking buttons, and recursively collects
 * text content and child data. It's designed to work with a specific DOM
 * structure where expandable elements are indicated by buttons.
 *
 * @param {NodeList|Array} anchors - A collection of anchor elements to process.
 * @returns {Promise<Array>} A promise that resolves to an array of objects,
 *                           each representing a root node and its nested data.
 *
 * @example
 * // Assuming 'document.querySelectorAll("a")' returns the relevant anchors
 * const anchors = document.querySelectorAll("a");
 * const hierarchicalData = await expandAndGatherData(anchors);
 */
async (anchors) => {
    // Define the recursive function to gather data
    async function expandAndGather(node) {
        const data = {
            text: node.textContent.trim(),
            children: [],
        };
        const childRegex = new RegExp('^children_of_.*')
        const collapseRegex = new RegExp('^Collapse')
    
        const button = node.parentElement.querySelector('button');  // Adjusted to look within the node itself
        // If the buttons exist and there isn't the text 'Collapse' in them...
        if (button && !collapseRegex.test(button.textContent.trim())) { 
                await button.click();
                await new Promise(resolve => setTimeout(resolve, 1000));
            } else {
                // Debug to check if the button is disabled or not found
                console.log('Button not found or disabled for node:', node);
            }

        // Adjusted to collect children directly from node.
        const ul = node.parentElement.querySelector('ul');
        console.log('ul:', ul);
        if (ul && childRegex.test(ul.id)) {
            const childNodes = Array.from(ul.children);
            const count = childNodes.length;

            if (count > 0) { // NOTE Because this JS is evaluated via API, this doesn't actually work.
                console.log('Child nodes found:', count)
            } else {
                console.log('No child nodes found for node:', node)
            };

            for (const child of childNodes) {
                const childData = await expandAndGather(child);
                const flatResults = results.flat()
                if (childData) {
                    data.children.push(childData);
                }
            }
        }
        return data;
    }

    const genTocRegex = new RegExp('^genToc_.*');  // Regex of top level nodes
    const rootNodes = [];  // Keep track of roots

    // Find root nodes - only those with parent IDs matching 'genToc_*'
    for (const anchor of anchors) {
        if (genTocRegex.test(anchor.parentElement.id)) {
            // Look for a button within the same parent element
            const button = anchor.parentElement.querySelector('button');
            if (button) {
                rootNodes.push(anchor);  // Store the anchor object
            }
        }
    }

    // Start gathering from the root nodes
    const results = [];
    for (const rootNode of rootNodes) {
        const nodeData = await expandAndGather(rootNode);
        results.push(nodeData);
    }
    return flatResults;
}
