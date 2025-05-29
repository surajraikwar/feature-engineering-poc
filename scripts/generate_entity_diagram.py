"""Script to generate an entity relationship diagram from YAML definitions."""
import yaml
from pathlib import Path
from typing import Dict, List, Optional
import graphviz

class EntityDiagramGenerator:
    """Generates entity relationship diagrams from YAML definitions."""
    
    def __init__(self, registry_path: Path):
        """Initialize with path to registry."""
        self.registry_path = registry_path
        self.entities = {}
        self.graph = graphviz.Digraph(
            'Entity Relationships',
            filename='entity_relationships.gv',
            format='png',
            graph_attr={
                'rankdir': 'LR',
                'splines': 'ortho',
                'nodesep': '0.8',
                'ranksep': '1.0',
                'fontname': 'Arial',
                'fontsize': '12',
            },
            node_attr={
                'shape': 'record',
                'style': 'filled',
                'fillcolor': 'lightblue',
                'fontname': 'Arial',
                'fontsize': '10',
            },
            edge_attr={
                'fontname': 'Arial',
                'fontsize': '9',
            }
        )
    
    def load_entities(self) -> None:
        """Load all entity definitions from YAML files."""
        entity_files = (
            list(self.registry_path.glob("**/*.yaml")) +
            list(self.registry_path.glob("**/*.yml"))
        )
        
        for file_path in entity_files:
            if file_path.name == "common_types.yaml":
                continue
                
            with open(file_path, 'r') as f:
                try:
                    content = yaml.safe_load(f)
                    if content and isinstance(content, list):
                        for entity in content:
                            if entity and 'name' in entity:
                                self.entities[entity['name']] = entity
                except yaml.YAMLError as e:
                    print(f"Error loading {file_path}: {e}")
    
    def _format_field(self, field: Dict) -> str:
        """Format a field for display in the node."""
        name = field.get('name', '')
        type_ = field.get('type', 'any')
        required = '*' if field.get('required', False) else ''
        return f"{name}: {type_}{required}"
    
    def _format_node(self, entity: Dict) -> str:
        """Format an entity as a node label."""
        name = entity['name']
        fields = entity.get('fields', [])
        
        # Format fields
        field_lines = ["<f0> " + name + " | "]
        for i, field in enumerate(fields, 1):
            field_lines.append(f"<f{i}> {self._format_field(field)}")
        
        # Join with newlines and add surrounding braces
        return "{" + " | ".join(field_lines) + "}"
    
    def generate_diagram(self, output_path: Path) -> None:
        """Generate the entity relationship diagram."""
        # Add nodes
        for entity_name, entity in self.entities.items():
            self.graph.node(
                entity_name,
                label=self._format_node(entity),
                shape='record',
                style='filled',
                fillcolor='lightblue',
            )
        
        # Add edges for relationships
        for entity_name, entity in self.entities.items():
            for rel in entity.get('relations', []):
                target = rel.get('to')
                if target in self.entities:
                    rel_type = rel.get('type', '')
                    label = f"{rel_type}"
                    if 'foreign_key' in rel:
                        label = f"{label}\n({rel['foreign_key']})"
                    
                    # Add edge with arrow based on relationship type
                    if rel_type == '1:1':
                        self.graph.edge(
                            f"{entity_name}:f0",
                            f"{target}:f0",
                            label=label,
                            arrowhead='none',
                            arrowtail='none',
                            dir='both',
                        )
                    elif rel_type == '1:many':
                        self.graph.edge(
                            f"{entity_name}:f0",
                            f"{target}:f0",
                            label=label,
                            arrowhead='crow',
                            arrowtail='none',
                            dir='forward',
                        )
                    elif rel_type == 'many:1':
                        self.graph.edge(
                            f"{target}:f0",
                            f"{entity_name}:f0",
                            label=label,
                            arrowhead='crow',
                            arrowtail='none',
                            dir='forward',
                        )
                    elif rel_type == 'many:many':
                        self.graph.edge(
                            f"{entity_name}:f0",
                            f"{target}:f0",
                            label=label,
                            arrowhead='crow',
                            arrowtail='crow',
                            dir='both',
                        )
        
        # Render the graph
        self.graph.render(
            filename=output_path / 'entity_relationships',
            format='png',
            cleanup=True,
            view=False
        )

def main():
    """Main function to generate the entity diagram."""
    registry_path = Path(__file__).parent.parent / "registry" / "entity"
    output_path = Path(__file__).parent.parent / "docs" / "diagrams"
    
    # Create output directory if it doesn't exist
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Generate the diagram
    generator = EntityDiagramGenerator(registry_path)
    generator.load_entities()
    generator.generate_diagram(output_path)
    print(f"Entity relationship diagram generated at: {output_path}/entity_relationships.png")

if __name__ == "__main__":
    main()
