<h1> **CRAWLER APPLICATION** </h1>
<h4> Application aim is to collect all interested data from each social media like: Facebook, Twitter and Youtube. </h2>
<h3> Facebook:</h3>
<h6> The process collect every each of existing page. It is started from sotrender liked pages and then exploring every single page liked and their liked pages and so on, and so on. </h6>
<h3> Youtube:</h3>
<h6> Application collect every existed channel. Whole algorithm explore more and more on base of featured channnels urls paramater, which indicates next channels. Rest of that looks the same as in case of facebook. </h6>
<h3> Twitter:</h3>
<h6> Application start its process from the given verified base by twitter itselfs and collects accounts seems to be companies or worth collected one (different case is for instance sports star, singer etc). To be qualified for datebase it must be performed certain conditions</h6>
<h3> Executing:</h3>
<h5> To run application it is neccessery specifing arguments. There are examples of executing application below:</h3>
<h6> $ python graph_retrieving.py facebook (without building new database)</h6>
<h6> $ python graph_retrieving.py facebook init (with building new one) - this command will start process for facebook and also construct database from scratch </h6>
<h6> You can change variable facebook to twitter or youtube </h6>
<h6> Algorithms are explained correctly in included folder UML.</h6>
<h2> REMEMBER TO EDIT CONFIGURATION FILE BEFORE EXECUTING!</h2>

