import asyncio
from dataclasses import dataclass, field
from typing import Optional, Any
from playwright.async_api import Page, ElementHandle
from datetime import datetime
import re


import pandas as pd

from utils.shared.sanitize_filename import sanitize_filename
from utils.shared.save_dataclass_to_csv_via_pandas import save_dataclass_to_csv_via_pandas
from logger.logger import Logger
logger = Logger(logger_name=__name__)


@dataclass
class TraversalState:
    """
    Maintains state during menu traversal
    """
    visited_nodes: set[str] = field(default_factory=set)
    expanded_nodes: set[str] = field(default_factory=set)
    depth_map: dict[str, int] = field(default_factory=dict)
    traversal_path: list[str] = field(default_factory=list)
    errors: list[dict[str, Any]] = field(default_factory=list)
    start_time: float = field(default_factory=lambda: datetime.now().timestamp())


@dataclass
class NodeData:
    """
    Represents collected data for a single node
    """
    text: str
    children: list['NodeData'] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    url: str = ""
    node_id: str = ""
    depth: int = 0


class WalkNestedMunicodeMenu:
    """
    Traverse nested menu structures using Playwright.
    NOTE: This is specifically made for Municode's nested menu structure, and may not work for other websites.
    TODO: Make this work for other websites.

    Example:
        async def main():
            from playwright.async_api import async_playwright
            
            async with async_playwright() as p:
                browser = await p.chromium.launch()
                page = await browser.new_page()
                
                # Navigate to your page
                await page.goto('your_url_here')
                
                # Create traverser instance
                traverser = MenuTraverser(page)
                
                # Start traversal
                results = await traverser.nested_menu('a[id^="genToc"]')
                
                # Process results
                await browser.close()
                return results

        if __name__ == "__main__":
            results = asyncio.run(main())
    """
    # Class constants
    MAX_DEPTH = 100
    MAX_WAIT_TIME = 5000  # milliseconds (5 Seconds)
    RETRY_INTERVAL = 100  # milliseconds (1/10 of a Second)
    MAX_RETRIES = MAX_WAIT_TIME // RETRY_INTERVAL # 5000 // 100 = 50 retries
    
    # Regex patterns
    PATTERN_GEN_TOC = re.compile(r'^genToc_.*')
    PATTERN_CHILDREN = re.compile(r'^children_of_.*')
    PATTERN_COLLAPSE = re.compile(r'^Collapse')
    PATTERN_X_PATH_BUTTON = 'xpath=../button'

    # JS selectors
    NODE_PARENT_ID_BUTTON_SELECTOR = """
    (node) => {
        return node.parentElement.querySelector('button');
        }
    """

    def __init__(self, page: Page, place_name: str, output_folder: str, debug: bool = True):
        self.page = page
        self.debug = debug
        self.place_name = place_name
        self.output_folder = output_folder
        self.state = TraversalState()


    async def nested_menu(self, root_selector: str) -> pd.DataFrame:
        """
        Walk nested menu structures using Playwright.
        Starts at the root node and recursively traverses the menu until all nodes are visited or the maximum depth is reached.
        NOTE: This is tailored for Municode's Table of Contents menu in a generic Library page, and may not work for other websites or paths.
        TODO: Make this work for other websites or paths.

        Args:
            root_selector: CSS selector for root menu elements
            
        Returns:
            pd.DataFrame of objects from the NodeData dataclass, where each object is a column and each row is a node.
        """
        root_anchors = await self._get_anchors(root_selector)
        try:
            # Walk the node data if the root node is valid. Otherwise, skip it.
            logger.info(f"Starting menu traversal for {self.place_name}...")
            results = [ 
                node_data for anchor in root_anchors 
                if (await self._is_valid_root_node(anchor))
                if (node_data := await self._traverse_node(anchor, 0))
            ]
            logger.info("Menu traversal completed. Logging and saving...")

            # Log the results
            await self._log_traversal_summary()

            # Save the results to a CSV file via pandas.
            df = save_dataclass_to_csv_via_pandas(results, filename=f"{sanitize_filename(self.page.url)}_menu_traversal_results.csv", return_df=True)
            return df

        except Exception as e:
            logger.error(f"Error during menu traversal: {e}")
            raise e


    async def _get_anchors(self, root_selector: str) -> list[ElementHandle]:
        """
        Retrieve anchor elements using the provided selector or a default pattern.

        Args:
            root_selector (str): The CSS selector to find anchor elements.

        Returns:
            list[ElementHandle]: A list of Playwright ElementHandles representing the found anchors.

        Raises:
            ValueError: If no elements are found using either the provided selector or the default pattern.
        """
        # Select the nodes with query selector
        logger.debug(f"Selecting anchors with root_selector {root_selector}")
        root_anchors = await self.page.query_selector_all(root_selector)

        if not root_anchors:
            # If we don't find anything with root_selector, use the class' default.
            logger.warning("root_selector value returned no values. Using class default PATTERN_GEN_TOC...")
            root_anchors = await self.page.query_selector_all(self.PATTERN_GEN_TOC)
            if not root_anchors:
                raise ValueError(f"""
                    No elements found matching selectors or PATTERN_GEN_TOC
                    root_selector: {root_selector}
                    PATTERN_GEN_TOC: {self.PATTERN_GEN_TOC}
                """)
        return root_anchors


    def _node_was_visited(self, node_id: str, depth: int) -> bool:
        """
        Check if a node was already visited.
        Return True if the node was visited. Otherwise, mark it as visited and return False.
        """
        if node_id in self.state.visited_nodes:
            logger.debug(f"Node {node_id} already visited")
            return True
        else:
            self.state.visited_nodes.add(node_id)
            self.state.depth_map[node_id] = depth
            logger.debug(f"Marked node {node_id} as visited at depth {depth}")
            return False


    def _depth_is_over_max_depth(self, node_id: str, depth: int) -> bool:
        """
        Check the depth against the maximum depth. 
        Return True if the depth is within the limit, False otherwise.
        """
        if depth > self.MAX_DEPTH:
            logger.warning(f"Max depth exceeded at node {node_id}")
            return True
        return False


    async def _build_dataclass_for_node(self, node: ElementHandle, node_id: str, depth: int, child_tup: tuple=None) -> NodeData | None:
        """
        Build a NodeData object for a given node in the menu structure.
        
        This function processes a single node, extracting relevant information and creating
        a NodeData object to represent it. It handles depth checking, visit tracking,
        and retrieves text and URL information for the node.
        Args:
            node (ElementHandle): The Playwright ElementHandle representing the current node.
            node_id (str): A unique identifier for the node.
            depth (int): The current depth of the node in the menu structure.
            child_tup (tuple, optional): A tuple containing (current_child_index, total_children)
                                         for logging purposes. Defaults to None.

        Returns:
            NodeData: An object containing the processed data for the node, or None if:
                      - The maximum depth has been exceeded
                      - The node has already been visited
        """
        child_text = f' child {child_tup[0]}/{child_tup[1]} of ' if child_tup is not None else ''
        logger.debug(f"Traversing{child_text} node '{node_id}' at depth '{depth}'")

        if self._node_was_visited(node_id, depth) or self._depth_is_over_max_depth(node_id, depth):
            return None

        node_text = await self._get_node_text(node)
        node_url = await self._get_node_url(node)
        logger.debug(f"Node {node_id}\ntext: {node_text}\nurl: {node_url}")

        # Initialize node data
        node_data = NodeData(
            text=node_text,
            node_id=node_id,
            depth=depth,
            url=node_url,
            metadata={
                'path': '/'.join(self.state.traversal_path),
                'timestamp': datetime.now().isoformat()
            }
        )
        logger.debug(f"Initialized NodeData for {node_id}")
        return node_data


    async def _traverse_node(self, node: ElementHandle, depth: int, child_tup: tuple=None) -> Optional[NodeData]:
        """
        Recursively traverse a single node and its children
        
        Args:
            node: Playwright ElementHandle for the current node
            depth: Current depth in the menu structure
            
        Returns:
            NodeData object or None if node should be skipped
        """
        try:
            node_id = await self._make_node_id(node)
            node_data = await self._build_dataclass_for_node(node, node_id, depth, child_tup)
            if not node_data:
                return None

            # Expand node if necessary
            if await self._should_expand_node(node):
                logger.debug(f"Attempting to expand node {node_id}")
                expand_success = await self._expand_node(node)
                node_data.metadata['expanded'] = expand_success
                logger.debug(f"Node {node_id} expansion {'successful' if expand_success else 'failed'}")
                
                if expand_success:
                    logger.debug(f"Processing children of node {node_id}")
                    child_container = await self._wait_for_child_container_to_load(node)

                    if child_container:
                        child_nodes = await self._get_child_nodes(child_container)
                        logger.debug(f"Found {len(child_nodes)} children for node {node_id}")

                        # Look for children and re-run traverse node over them.
                        node_data.children = [
                            child_data for i, child in enumerate(child_nodes)
                            if (child_data := await self._traverse_node(child, depth + 1, child_tup=(i, len(child_nodes),)))
                        ]

            logger.debug(f"Completed traversal of node {node_id}")
            return node_data

        except Exception as e:
            logger.error(f"Error traversing node {node_id}: {e}")
            self.state.errors.append({
                'node_id': node_id if 'node_id' in locals() else 'unknown',
                'depth': depth,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            })
            raise e


    async def _expand_node(self, node: ElementHandle) -> bool:
        """
        Attempt to expand a menu node
        
        Args:
            node: ElementHandle for the node to expand
            
        Returns:
            bool indicating success of expansion
        """
        button = await node.query_selector(self.PATTERN_X_PATH_BUTTON)
        logger.debug(f" _expand_node button: {button}\ntype: {type(button)}")
        if not button:
            return True  # No button means no expansion needed

        # Check if already expanded
        button_text = await button.text_content()
        if button_text and self.PATTERN_COLLAPSE.match(button_text.strip()):
            return True

        retry_count = 0
        while retry_count < self.MAX_RETRIES:
            try:
                # Click the button
                await button.click()
                
                # Wait for expansion

                if await self._verify_expansion(node):
                    node_id = await self._make_node_id(node)
                    self.state.expanded_nodes.add(node_id)
                    return True

            except Exception as e:
                logger.warning(f"Expansion attempt {retry_count + 1} failed: {e}")
                
            await asyncio.sleep(self.RETRY_INTERVAL / 1000)  # Convert to seconds
            retry_count += 1

        return False


    async def _verify_expansion(self, node: ElementHandle) -> bool:
        """
        Verify that a node has been successfully expanded.
        """
        try:
            # Use JavaScript to verify DOM changes.
            # NOTE JS selects all sub-lists in the menu and checks if they exist and if they have children. 
            expanded = await self.page.evaluate("""
                node => {
                    const ul = node.parentElement.querySelector('ul');
                    return ul && ul.children.length > 0;
                }
            """, node)
            return bool(expanded)
        except Exception:
            return False


    async def _wait_for_child_container_to_load(self, node: ElementHandle) -> Optional[ElementHandle]:
        """
        Wait for and return the child container if it exists.
        """
        try:
            # Use JavaScript to get nodes.
            # NOTE JS selects all sub-lists in the menu and returns any that has 'children_of' in its aria-controls. 
            container = await node.evaluate("""
                node => {
                    const button = node.parentElement.querySelector('button');
                    if (!button) { // Check if the button exists.
                        return null
                    }
                    const ariaControls = button.getAttribute('aria-controls');
                    if (!ariaControls) { // Check if aria-controls exists as an attribute.
                        return null
                    }
                    // If aria-controls has 'children_of_' of in them, return the element, else return nothing.
                    if (ariaControls.match('^children_of_.*')) { 
                        const controlledElement = document.getElementById(ariaControls);
                        return controlledElement || null;
                    }
                    return null;
                }
            """)
            if container:
                return await node.query_selector('xpath=../ul')
            return None
        except Exception as e:
            logger.error(f"Error waiting for child container: {e}")
            return None


    async def _get_child_nodes(self, container: ElementHandle) -> list[ElementHandle]:
        """
        Get all child nodes from a container.
        """
        try:
            return await container.query_selector_all('li > a')
        except Exception as e:
            logger.error(f"Error getting child nodes: {e}")
            return []


    async def _is_valid_root_node(self, node: ElementHandle) -> bool:
        """
        Validate if a node is a valid root node.
        """
        try:
            valid = await node.evaluate("""
                node => {
                    const parent = node.parentElement;
                    // Check if the parent node exists, has 
                    return parent &&
                           parent.id.match(/^genToc.*/) &&
                           parent.querySelector('button') !== null;
                }
            """)
            return bool(valid)
        except Exception:
            return False


    async def _make_node_id(self, node: ElementHandle) -> str:
        """
        Get a unique identifier for a node.
        """
        try:
            node_id = await node.evaluate('node => node.parentElement.id')
            return str(node_id) if node_id else f"node_{id(node)}"
        except Exception:
            return f"node_{id(node)}"


    async def _get_node_url(self, node: ElementHandle) -> str:
        """
        Get a unique identifier for a node
        """
        try:
            url = await node.as_element().get_attribute('href')
            return str(url) if url else None
        except Exception as e:
            logger.error(f"Error getting node URL: {e}")
            return None


    async def _get_node_text(self, node: ElementHandle) -> str:
        """
        Get the text content of a node
        """
        try:
            return (await node.text_content() or '').strip()
        except Exception:
            return ''


    async def _should_expand_node(self, node: ElementHandle) -> bool:
        """
        Determine if a node should be expanded.
        """
        try:
             # Get the button element within the node, if it exists
            button = await node.evaluate(self.NODE_PARENT_ID_BUTTON_SELECTOR)
            if not button:
                logger.debug("Could not find button element. Returning False")
                return False
            else:
                logger.debug("Found button element. Getting text...")
                button_text = await node.evaluate("""
                    button => {
                        const buttonElement = button.parentElement.querySelector('button');
                        return buttonElement ? buttonElement.textContent.trim() : '';
                    }
                """, button)
                return button_text and not self.PATTERN_COLLAPSE.match(button_text.strip())

        except Exception as e:
            logger.error(f"Error in _should_expand_node: {e}")
            return False


    async def _log_traversal_summary(self):
        """
        Log summary of traversal operation.
        """
        duration = datetime.now().timestamp() - self.state.start_time
        logger.info(f"""
                    Traversal Summary:
                    Total nodes visited: {len(self.state.visited_nodes)}
                    Successfully expanded: {len(self.state.expanded_nodes)}
                    Max depth reached: {max(self.state.depth_map.values(), default=0)}
                    Errors encountered: {len(self.state.errors)}
                    Duration: {duration:.2f} seconds""", f=True)

        if self.state.errors:
            logger.error("Error details:")
            for error in self.state.errors:
                logger.error(f"Node {error['node_id']} at depth {error['depth']}: {error['error']}")
        return

