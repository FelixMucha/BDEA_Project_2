document.querySelectorAll('#file_list li').forEach(function(listItem) {
    listItem.addEventListener('click', function(event) {
        event.preventDefault();
        var image = this.querySelector('.image');
        var wasHidden = image.style.display === 'none';
        image.style.display = wasHidden ? 'block' : 'none';
    });
});