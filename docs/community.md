---
layout: default
title: Community
nav_order: 99
---

# Community

Soda SQL is open-source software distributed under the Apache License 2.0.

<p align="left">
  <a href="https://github.com/sodadata/soda-sql/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-Apache%202-blue.svg" alt="License: Apache 2.0"></a>
  <a href="https://join.slack.com/t/soda-community/shared_invite/zt-m77gajo1-nXJF7JtbbRht2zwaiLb9pg"><img alt="Slack" src="https://img.shields.io/badge/chat-slack-green.svg"></a>
  <a href="https://pypi.org/project/soda-sql/"><img alt="Pypi Soda SQL" src="https://img.shields.io/badge/pypi-soda%20sql-green.svg"></a>
  <a href="https://github.com/sodadata/soda-sql/actions/workflows/build.yml"><img alt="Build soda-sql" src="https://github.com/sodadata/soda-sql/actions/workflows/build.yml/badge.svg"></a>
</p>

## Contribute

We welcome and encourage any kind of contributions and suggestions for improvement! 

Soda SQL project: [github.com/sodadata/soda-sql](https://github.com/sodadata/soda-sql/)

Join the conversation in [Soda's Slack community](https://join.slack.com/t/soda-community/shared_invite/zt-m77gajo1-nXJF7JtbbRht2zwaiLb9pg).

Contact us using [GitHub discussions](https://github.com/sodadata/soda-sql/discussions) to:
* ask a question
* post a problem
* share your feedback
* suggest an improvement or contribution 

Write [documentation](#documentation-guidelines) for Soda SQL.

Read more about how to [contribute to Soda SQL](https://github.com/sodadata/soda-sql/blob/main/CONTRIBUTING.md). 


## Roadmap

We have a long and exciting path ahead for developing Soda SQL and beyond. Check out our 
[project roadmap](https://github.com/sodadata/soda-sql/projects/1) on GitHub.

Do you have a feature request? Want to help us develop CLI tools for other SQL engines?  [Create an issue](https://github.com/sodadata/soda-sql/issues/new) on GitHub.



## Documentation guidelines

Join us in our mission to help users become productive and confident using Soda software.  

The following outlines the workflow to contribute.
1. [Set up docs tooling](#set-up-docs-tooling) locally on your machine and clone the GitHub repo.
2. Create a new branch for your work. Include the word `docs` in the name of your branch.
3. Follow the [style guidelines](#style-guidelines) to edit existing or write new content using [markdown](#use-jekyll-markdown).
4. Adjust the nav order of any new files in the header of the file.
5. Spell check your content (select all and copy to Google Docs for a thorough check) and test all links.
6. Commit your contributions, create a pull request, and request a review, if you wish.
7. When all tests pass, merge your pull request.
8. Celebrate your new life as a published author! 

### Set up docs tooling

Soda uses the following tools to build and publish documentation.
- [GitHub](https://github.com/sodadata/soda-sql) to store content
- [Jekyll](https://jekyllrb.com/docs/) to build and serve content
- [Just the Docs](https://pmarsceill.github.io/just-the-docs/) to apply a visual theme

To contribute to Soda documentation, set up your local system to author and preview content before committing it.

1. Jekyll requires Ruby 2.4.0 or higher. If necessary, upgrade or install Ruby locally from the command-line.
```shell
brew install ruby
``` 
2. Add Ruby path to your `~/.bash_profile`.
```shell
$ echo 'export PATH="/usr/local/opt/ruby/bin:$PATH"' >> ~/.bash_profile
```
3. Relaunch your command-line interface, then check your Ruby setup.
```shell
$ which ruby
/usr/local/opt/ruby/bin/ruby
$ ruby -v
ruby 3.0.0p0 (2020-12-25 revision 95aff21468) [x86_64-darwin20]
```
4. Install two Ruby gems: bundler and jekyll.
```shell
$ gem install --user-install bundler jekyll
```
5. Add gem path to your `~/.bash_profile`.
```shell
$ echo 'export PATH="/Users/Janet/.gem/ruby/3.0.0/bin:$PATH"' >> ~/.bash_profile
```
6. Check that `GEM PATHS` point to your home directory.
```shell
$ gem env
...
GEM PATHS:
- /usr/local/lib/ruby/gems/3.0.0
- /Users/me/.gem/ruby/3.0.0
- /usr/local/Cellar/ruby/3.0.0_1/lib/ruby/gems/3.0.0
```
7. Clone the soda-sql repo from GitHub.
8. Open the cloned repo locally in your favorite code editor such as Visual Code or Sublime.
9. From the command-line, navigate to the directory that contains the cloned repo, then run the following to build and serve docs locally.
```shell
$ bundle exec jekyll serve
```
10. In a browser, navigate to [http://localhost:4000/soda-sql/](http://localhost:4000/soda-sql/) to see a preview of the docs site locally. Make changes to docs files in your code editor, save the files, then refresh your browser to preview your changes.


### Troubleshoot

**Problem:** When I run `bundle exec jekyll serve`, I get this error:
```
$ bundle exec jekyll serve
...
/usr/local/lib/ruby/gems/3.0.0/gems/jekyll-4.2.0/lib/jekyll/commands/serve/servlet.rb:3:in `require': cannot load such file -- webrick (LoadError)
```
**Solution:**
Ruby 3.0.0 no longer includes the `webrick 1.7.0` gem that jekyll needs to build and serve docs. Install the gem locally.
1. In your code editor, open `docs` > `Gemfile`.
2. Add the following to the file and save.
```ruby
$ gem "webrick"
```
3. From the command-line, run `bundle install`.
4. Run `bundle exec jekyll serve`. 
5. Before you commit your changes to the repo, be sure to discard your changes to the Gemfile. Just add the `webrick` gem again the next time you open a new branch.

### Style guidelines

Soda uses the [Splunk Style Guide](https://docs.splunk.com/Documentation/StyleGuide/current/StyleGuide/Howtouse) for writing documentation. For any questions about style or language that are not listed below, refer to the Splunk Style Guide for guidance.

Language:
- Use American English.
- Use [plain](https://docs.splunk.com/Documentation/StyleGuide/current/StyleGuide/Technicallanguage) language. Do not use jargon, colloquialisms, or meme references.
- Use [unbiased](https://docs.splunk.com/Documentation/StyleGuide/current/StyleGuide/Inclusivity) language. For example, instead of "whitelist" and "blacklist", use "passlist" and "denylist".
- Avoid writing non-essential content such as welcome messages or backstory.
- When referring to a collection of files, use "directory", not "folder".
- Use "you" and "your" to refer to the reader. 
- Do not refer to Soda, the company, as participants in documentation: "we", "us", "let's", "our".
- Use [active voice](https://docs.splunk.com/Documentation/StyleGuide/current/StyleGuide/Activeandpresent).
- Use present tense and imperative mood. See the [Set up docs tooling](#set-up-docs-tooling) section above for an example.
- Avoid the subjunctive mood: "should", "would", "could". 
- Make the language of your lists [parallel](https://ewriteonline.com/how-and-why-to-make-your-lists-parallel-and-what-does-parallel-mean/). 

Good practice:
- Never write an FAQ page or section. FAQs are a randomly organized bucket of content that put the burden on the reader to find what they need. Instead, consciously think about when and where a user needs the information and include it there.
- Include code snippets and commands. 
- Limit inclusion of screencaps. These images are hard to keep up-to-date as the product evolves.
- Include diagrams.
- Do not use "Note:" sections. Exception: to indicate incompatibility or a known issue. 
- Use includes rather than repeat or re-explain something.

Formatting:
- Use **bold** for the first time you mention a product name or feature in a document. See [Warehouse YAML]({% link documentation/warehouse.md %}) for an example. Otherwise, use it sparingly. Too much bold font renders the format meaningless.
- Use *italics* sparingly for emphasis on the negative. For example, "Do *not* share your login credentials."
- Use sentence case for all titles and headings.
- Use H1 headings for the page title. Use H2 and H3 as subheadings. Use H4 headings to introduce example code snippets.
- Never stack headings with no content between them. Add content or remove a heading, likely the latter so as to avoid adding non-essential text.
- Use [bulleted lists](https://docs.splunk.com/Documentation/StyleGuide/current/StyleGuide/Bulletlists) for non-linear lists.
- Use [numbered lists](https://docs.splunk.com/Documentation/StyleGuide/current/StyleGuide/Tasklists) for procedures or ordered tasks.
- Use relative links to link to other files or sections in Soda documentation.
- Use hard-coded links to link to external sources.
- Liberally include links to the [Glossary]({% link documentation/glossary.md %}), but only link the first instance of a term on a page, not all instances.

Content:
- Categorize your new content according to the following macro groups:
   - Concepts - content that explains in general, without including procedural steps. Characterized by a title that does not use present tense imperative such as, "How Soda SQL works" or "Metrics".
   - Tasks - content that describes the steps a user takes to complete a task or reach a goal. Characterized by a title that is in present tense imperative such as, "Install Soda SQL" or "Apply filters".
   - Reference - content that presents lists or tables of reference material such as error codes or glossary.
- Produce content that focuses on how to achieve a goal or solve a problem and, insofar as it is practical, is inclusive of all products. Avoid creating documentation that focuses on how to use a single product. For example, instead of writing two documents -- one for "Troubleshoot Soda SQL" and one for "Troubleshoot Soda Cloud" -- write one Troubleshoot document that offers guidance for both tools. 
- Remember that Every Page is Page One for your reader. Most people enter your docs by clicking on the result of a Google search, so they could land anywhere and you should assume your new page is the first page that a new reader lands on. Give them the context for what they are reading, lots of "escape hatches" to the glossary or pre-requisite procedures, and instructions on what to read next in a "Go further" section at the bottom of all Concept or Task pages. 

### Use Jekyll markdown 

Kramdown is the default markdown renderer that Jekyll uses. 
{% raw %}
Insert image:

Add a `png` file of your logically-named image to the `docs/assets/images` directory, then add this code:
```
![scan-anatomy](../assets/images/scan-anatomy.png){:height="440px" width="440px"}
```

Includes:

Add a markdown file of your logically-named include content to the `docs/_includes` directory, then add this code:
```
{% include run-a-scan.md %}
```

Relative links:
```
[warehouse yaml]({% link documentation/warehouse.md %})

[airflow_bash.py](../../examples/airflow_bash.py)
```

Link to anchor:

```
[warehouse yaml]({% link documentation/warehouse.md %}#to-anchor)
```

Link to section on same page:

```
[example](#example-tests-using-a-column-metric)
```

External link:

```
[Wikipedia](https://en.wikipedia.org)
```

Comment out:

```
<!-- This content does not display on the web page. -->
```

{% endraw %}




