from dataclasses import dataclass, field
from typing import Optional

@dataclass
class HtmlElement:
    """
    Dataclass to encapsulate all the information needed to represent a single HTML element,
    including its structure, content, and styling.

    Attributes:
        id (str): A unique identifier for the element.
        tag_name (str): The name of the HTML tag (e.g., "div", "p", "a").
        attributes (dict[str, str]): Key-value pairs of HTML attributes.
        inner_text (str): The text content of the element, if present.
        inner_html (str): The HTML content inside the element, if present.
        class_list (list[str]): List of CSS classes applied to the element.
        parent_id (Optional[str]): The ID of the parent element, if any.
        children_ids (list[str]): List of IDs of child elements.
        style (dict[str, str]): CSS styles applied to the element.
        is_visible (bool): Visibility state of the element.
        bounding_box (Optional[dict[str, float]]): The element's position and dimensions.
        href (Optional[str]): The URL for anchor tags.
        src (Optional[str]): The source URL for elements like images.
        alt (Optional[str]): Alternative text for images.
        value (Optional[str]): The value attribute for form elements.
        name (Optional[str]): The name attribute for form elements.
        type (Optional[str]): The type attribute for input elements.
        placeholder (Optional[str]): The placeholder text for input elements.
    """

    id: str
    tag_name: str
    attributes: dict[str, str] = field(default_factory=dict)
    inner_text: str = ""
    inner_html: str = ""
    class_list: list[str] = field(default_factory=list)
    parent_id: Optional[str] = None
    children_ids: list[str] = field(default_factory=list)
    style: dict[str, str] = field(default_factory=dict)
    is_visible: bool = True
    bounding_box: Optional[dict[str, float]] = None
    href: Optional[str] = None
    src: Optional[str] = None
    alt: Optional[str] = None
    value: Optional[str] = None
    name: Optional[str] = None
    type: Optional[str] = None
    placeholder: Optional[str] = None
