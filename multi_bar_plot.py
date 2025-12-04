import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import numpy as np

def multi_bar_plot(Data, accuracy_type, title, xlabel, ylabel):
    """
    Generates a grouped bar chart where each category has a distinct color,
    and bars within the category fade to white as the index 'j' increases.

    Args:
        Data (dict): Keys are category names, Values are lists of top-j accuracies [0-1].
        accuracy_type (list): List of integers (e.g., [1, 5, 10]) corresponding to the values.
        title (str): Chart title.
        xlabel (str): X-axis label.
        ylabel (str): Y-axis label.
    """
    
    # 1. Setup Data and Layout
    categories = list(Data.keys())
    n_cat = len(categories)
    n_acc = len(accuracy_type)
    
    # Basic layout parameters
    x_indices = np.arange(n_cat)  # The center positions for categories
    total_width = 0.8             # Width of the whole group of bars
    bar_width = total_width / n_acc
    
    # Get a qualitative colormap (one distinct color per category)
    # 'tab10' is good for up to 10 distinct categories
    cmap = plt.get_cmap('tab10')

    fig, ax = plt.subplots(figsize=(10, 6))

    # 2. Loop through Categories (Outer Loop)
    for i, cat in enumerate(categories):
        values = Data[cat]
        
        # Validation
        if len(values) != n_acc:
            raise ValueError(f"Category '{cat}' data length does not match accuracy_type length.")

        # Get base color for this category
        base_color = cmap(i % 10) 
        base_rgb = mcolors.to_rgb(base_color)

        # 3. Loop through Accuracy Levels (Inner Loop - The Bars)
        for j, val in enumerate(values):
            # Calculate position to center the group on the tick
            offset = (j - n_acc / 2 + 0.5) * bar_width
            pos = x_indices[i] + offset
            
            # --- Gradient Logic ---
            # Calculate "whiteness" factor based on index j.
            # j=0 -> Factor 0 (Pure Color)
            # j=max -> Factor approaches 0.8 (Very light, but not invisible)
            white_factor = (j / max(n_acc - 1, 1)) * 0.7
            
            # Mix base color with white
            # New_Color = Base * (1 - factor) + White * factor
            mixed_color = [c * (1 - white_factor) + 1.0 * white_factor for c in base_rgb]
            
            # Plot the bar
            ax.bar(pos, val, width=bar_width, color=mixed_color, edgecolor='white', linewidth=0.7)

    # 4. Styling and Labels
    ax.set_title(title, fontsize=14, fontweight='bold', pad=15)
    ax.set_xlabel(xlabel, fontsize=12)
    ax.set_ylabel(ylabel, fontsize=12)
    
    # Configure X-axis ticks
    ax.set_xticks(x_indices)
    ax.set_xticklabels(categories, fontsize=11)
    
    # Y-axis limit and grid
    ax.set_ylim(0, 1.05)
    ax.grid(axis='y', linestyle='--', alpha=0.5, zorder=0)

    # 5. Create a Neutral Legend
    # Since colors change per category, the legend explains the *shade* (Top-k)
    # using grayscale proxies.
    legend_patches = []
    for j, acc_k in enumerate(accuracy_type):
        # Create a matching gray gradient for the legend
        gray_val = 0.3 + (j / max(n_acc - 1, 1)) * 0.6
        color_patch = (gray_val, gray_val, gray_val)
        
        patch = plt.Rectangle((0,0), 1, 1, color=color_patch, label=f'Top-{acc_k}')
        legend_patches.append(patch)
        
    ax.legend(handles=legend_patches, title="Accuracy Level", loc='lower right')

    plt.tight_layout()
    plt.show()

# --- Example Usage ---

# 1. Define the accuracy levels (Top-k)
acc_levels = [1, 5, 7,15, 20]

# 2. Define the data (Accuracy between 0 and 1)
data_example = {
    "ResNet-50": [0.65, 0.75, 0.85, 0.92, 1.0],
    "VGG-16":    [0.60, 0.70, 0.80, 0.88, 1.0],
    "AlexNet":   [0.50, 0.60, 0.70, 0.80, 0.9],
    "AlexNetto":   [0.50, 0.60, 0.70, 0.80, 0.9],
}

# 3. Call the function
multi_bar_plot(
    Data=data_example, 
    accuracy_type=acc_levels, 
    title="Model Performance per Dataset (Top-k Accuracy)", 
    xlabel="Model Architecture", 
    ylabel="Accuracy Score"
)